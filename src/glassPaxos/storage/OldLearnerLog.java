package glassPaxos.storage;

import glassPaxos.Configuration;
import glassPaxos.SimpleLogger;
import glassPaxos.client.TestEntry;
import glassPaxos.interfaces.LearnerLogCbInterface;
import glassPaxos.learner.Learner;
import glassPaxos.network.NodeIdentifier;
import glassPaxos.network.messages.ClientRequest;
import glassPaxos.network.messages.DecisionMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;

public class OldLearnerLog implements Iterable<DecisionMessage> {
	private SimpleLogger LOG = SimpleLogger.getLogger("LearnerLog");
	private final String path;
	private static final long MAX_LOG_SIZE = 1048576L * 200L;  //200M

	private SequentialLog log = null;
	private DecisionMessage lastMessage = null;
	private GarbageCollectThread gcThread = new GarbageCollectThread();
	private LearnerLogCbInterface logCbInterface;

	@Override
	public Iterator<DecisionMessage> iterator() {
        return new LearnerLogIterator();
    }

	public OldLearnerLog(String path, LearnerLogCbInterface cb){
		this.path = path;
		this.logCbInterface = cb;
		gcThread.start();
	}

	private class LogTask {
		public LogTask(int op, long index) {
			this.op = op;
			this.index = index;
		}
		int op;
		long index;
	}
	
	/*
	 * If a log entry is out of order, LearnerLog will create a new
	 * sequential log. Therefore, too many out-of-order log can hurt
	 * performance.
	 */
	private void createNewSequentialLogWhenNecessary(DecisionMessage msg){
		if(log == null || 
				(lastMessage!=null && msg.getIndex() < lastMessage.getIndex())){
			try{
				Thread.sleep(1);
			}
			catch(InterruptedException e){
				LOG.warning(e);
			}
			String seqLogPath = path + File.separator + System.currentTimeMillis();
			if(log != null)
				log.close();
			LOG.debug("create new log %s\n", seqLogPath);
			log = new SequentialLog(seqLogPath);
		}
	}
	
	/*
	 * Add a log entry. Entries can be logged out of order.
	 */
	public void update(DecisionMessage msg){
		createNewSequentialLogWhenNecessary(msg);
		log.append(msg);
		lastMessage = msg;
	}
	
	/*
	 * Sync data to disk.
	 */
	public void doSync(){
		if(log != null)
			log.sync();
	}
	
	public void close(){
		if(log != null)
			log.close();
		gcThread.terminate();
	}

	/*
	 * Delete all log entries <= index.
	 * Note that this function is just a hint that
	 * those log entries can be deleted but actual 
	 * deletion may not happen immediately.
	 * The user cannot assume those log entries will
	 * not appear in the next iteration.
	 */
	public void garbageCollect(long index){
		//gcThread.addTask(index);
		gcThread.addTask(new LogTask(1, index));
	}

	public void readLogChunk(long index){
		//gcThread.addTask(index);
		gcThread.addTask(new LogTask(0, index));
	}

	public void sync() {
		gcThread.addTask(new LogTask(2, 0L));
	}

	private void doGarbageCollect(long index){
		File[] seqLogDirs = new File(path).listFiles();
		for(int i=0; i<seqLogDirs.length; i++){
			SequentialLog tmp = new SequentialLog(seqLogDirs[i].getPath());
			tmp.garbageCollect(index);
		}
	}

	private void doReadLogChunk(long index){
		logCbInterface.batchProcessLearnerLog(index);
	}

	private class GarbageCollectThread extends Thread{
		//private LinkedBlockingQueue<Long> queue = new LinkedBlockingQueue<Long>(100);
		private LinkedBlockingQueue<LogTask> queue = new LinkedBlockingQueue<LogTask>(100);
		private boolean running = true;
		
		//public void addTask(long index){
		public void addTask(LogTask task){
			try{
				//queue.put(index);
				if (queue.size() < 100) {
					int chunkSz = Configuration.maxPendingCatchupSync * Configuration.maxSyncDiff;
					if (0 == task.op && 0 == (task.index-1)%chunkSz ||
							task.op>0) {
						queue.put(task);
						//LOG.info("addTask=>(op=%d, index=%d)\n", task.op, task.index);
					} else {
						LOG.error("learner intends to add wrong task(op=%d, index=%d)\n", task.op, task.index);
					}
				} else {
					LOG.info("queue is full, addTask (op=%d, index=%d) later\n", task.op, task.index);
				}
			}
			catch(InterruptedException e){
				LOG.error(e);
				System.exit(-1);
			}
		}
		
		public void terminate(){
			running = false;
			this.interrupt();
		}
		
		public void run(){
			while(running){
				try{
					//long index = queue.take();
					LogTask t = queue.take();
					switch (t.op) {
						case 0:
							//LOG.info("=>doReadLogChunk index=%d\n", t.index);
							doReadLogChunk(t.index);
							break;
						case 1:
							//LOG.info("=>doGarbageCollect index=%d\n", t.index);
							doGarbageCollect(t.index);
							break;
						case 2:
							long tm1 = System.nanoTime();
							doSync();
							LOG.info("  ==>doSync time %.1f ms\n", (System.nanoTime()-tm1)/1000000.0);
							break;
						default:
							LOG.error("error LogTask op=%d\n", t.op);
							break;
					}
				}
				catch(InterruptedException e){
					LOG.debug(e);
				}
			}
		}
		
	}

	/*
	 * The iterator sorts all entries even if previously they 
	 * were logged out of order.
	 * The iterator does not de-duplicate entries with same
	 * index. 
	 */
	private class LearnerLogIterator implements Iterator<DecisionMessage> {

		private Iterator<DecisionMessage> [] iterators = null;
		private DecisionMessage[] heads = null;
		
		public LearnerLogIterator() {
			File[] seqLogDirs = new File(path).listFiles();
			iterators = new Iterator[seqLogDirs.length];
			heads = new DecisionMessage[seqLogDirs.length];
			for(int i=0; i<iterators.length; i++){
				iterators[i] = new SequentialLog(seqLogDirs[i].getPath()).iterator();
				if(iterators[i].hasNext())
					heads[i] = iterators[i].next();
			}
		}
		
		
		
		
			
		public boolean hasNext() {
			for(int i=0; i<heads.length; i++){
				if(heads[i]!=null)
					return true;
			}
			return false;
		}
			
		public DecisionMessage next() {
			if(this.hasNext()) {
				//do a merge sort on multiple sequential logs
			    DecisionMessage ret = null;
			    int index = -1;
			    for(int i=0; i<heads.length; i++){
			    	if(heads[i] == null) continue;
			    	if(ret == null){
			    		ret = heads[i];
			    		index = i;
			    	}
			    	else if(ret.getIndex() > heads[i].getIndex()){
			    		ret = heads[i];
			    		index = i;
			    	}
			    }
			    if(iterators[index].hasNext()){
			    	heads[index] = iterators[index].next();
			    }
			    else{
			    	heads[index] = null;
			    }
			    return ret;
			}
			throw new NoSuchElementException();
		}
			
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	
	private class SequentialLog implements Iterable<DecisionMessage>{
		private FileOutputStream currentFile = null;
		private DataOutputStream current = null;
		private final String path;
		private final byte[] bytes = new byte[Configuration.MAX_MSG_SIZE];
		private final ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);
		
		private SequentialLog(String path){
			this.path = path;
			File p = new File(path);
			p.mkdir();
		}
		
		@Override
		public Iterator<DecisionMessage> iterator() {
	        return new SequentialLogIterator();
	    }
		
		private long extractNumber(String name) {
            long i = -1;
            try {
                int s = name.indexOf('.') + 1;
                String number = name.substring(s);
                i = Long.parseLong(number);
            } catch(Exception e) {
                i = -1; // if filename does not match the format
                       // then default to 0
            }
            return i;
        }
		
		private File[] listFiles(){
			File f = new File(path);
			File [] ret = f.listFiles();
			Arrays.sort(ret, new Comparator<File>() {
	            @Override
	            public int compare(File o1, File o2) {
	                long n1 = extractNumber(o1.getName());
	                long n2 = extractNumber(o2.getName());
	                return (int)(n1 - n2);
	            }

	            
	        });
			return ret;
		}
		
		public void garbageCollect(long index){
			File [] logs = listFiles();
			int lastLogToDelete = -1;
			for(int i=0; i<logs.length; i++){
				File f = logs[i];
				long startIndex = extractNumber(f.getName());
				if(startIndex <= index){
					lastLogToDelete = i - 1;
				}
			}
			for(int i=0; i<lastLogToDelete; i++){
				LOG.debug("Garbage collect %s\n", logs[i].getPath());
				logs[i].delete();
			}
		}
		
		private void createNewLogWhenNecessary(long index) {
			try{
				if(currentFile == null || currentFile.getChannel().size() >= MAX_LOG_SIZE){
					if(currentFile != null){
						current.close();
						currentFile.close();
					}
					String fileName = path + File.separator + "log." + index;
					currentFile = new FileOutputStream(fileName);
					current = new DataOutputStream(new BufferedOutputStream(currentFile));
				}
			}
			catch(IOException e){
				LOG.error(e);
				System.exit(-1);
			}
		}
		
		public void append(DecisionMessage msg){
			createNewLogWhenNecessary(msg.getIndex());
			byteBuf.clear();
			msg.serialize(byteBuf);
			try{
				current.writeInt(byteBuf.readableBytes());
				current.write(bytes, 0, byteBuf.readableBytes());
			}
			catch(IOException e){
				LOG.error(e);
				System.exit(-1);
			}
		}
		
		public void sync(){
			try{
				if(currentFile != null){
					current.flush();
					currentFile.getFD().sync();
				}
			}
			catch(IOException e){
				LOG.error(e);
				System.exit(-1);
			}
		}
		
		public void close(){
			try{
				if(current != null){
					current.close();
				}
			}
			catch(IOException e){
				LOG.error(e);
				System.exit(-1);
			}
		}
		
		private class SequentialLogIterator implements Iterator<DecisionMessage> {

			private File[] logs = null;
			private int currentLog = 0;
			private DataInputStream dis = null;
			private DecisionMessage next = null;
			private final byte[] bytes = new byte[Configuration.MAX_MSG_SIZE];
			private final ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);
			
			public SequentialLogIterator() {
				this.logs = listFiles();
				if(logs.length > 0){
					try {
						//dis = new DataInputStream(new FileInputStream(logs[currentLog]));
						dis = new DataInputStream(new BufferedInputStream(new FileInputStream(logs[currentLog]), 65536));
					} catch (FileNotFoundException e) {
						LOG.error(e);
					}
				}
				readNextEntry();
			}
			
			private void readNextEntry(){
				try {
					if((dis==null || dis.available() == 0) && (logs.length == 0 || currentLog == logs.length -1) ){
						next = null;
						return;
					}
					if(dis.available() == 0){
						dis.close();
						currentLog ++ ;
						//dis = new DataInputStream(new FileInputStream(logs[currentLog]));
						dis = new DataInputStream(new BufferedInputStream(new FileInputStream(logs[currentLog]), 65536));
					}
					if(dis.available() > 0){
						int size = dis.readInt();
						dis.readFully(bytes, 0, size);
						byteBuf.clear();
						byteBuf.writerIndex(size);
						DecisionMessage msg = new DecisionMessage();
						msg.deserialize(byteBuf);
						next = msg;
					}
					else{
						next = null;
					}
				} catch (IOException e) {
					LOG.warning(e);
					next = null;
				}
			}
			
			
				
			public boolean hasNext() {
				return next != null;
			}
				
			public DecisionMessage next() {
				if(this.hasNext()) {
				    DecisionMessage ret = next;
				    readNextEntry();
				    return ret;
				}
				throw new NoSuchElementException();
			}
				
			public void remove() {
				throw new UnsupportedOperationException();
			}
		}
	}


	public static void main(String []args) throws Exception{
		Configuration.addActiveLogger("LearnerLog", SimpleLogger.INFO);
		File logDir = new File("/Users/rongshi/ywlab/glassPaxos/logtest");
		for(File f: logDir.listFiles()){
			f.delete();
		}
		Learner learner = new Learner(); //todo: check this
		OldLearnerLog log = new OldLearnerLog("/Users/rongshi/ywlab/glassPaxos/logtest", learner);
		NodeIdentifier node = new NodeIdentifier(NodeIdentifier.Role.ACCEPTOR, 1);
		NodeIdentifier client = new NodeIdentifier(NodeIdentifier.Role.CLIENT, 2);
		int op;
		String path = "LearnerLogTest";
		int numDec = 70000;
		System.out.printf("create LearnerLog and will append %d entries\n", numDec);
		for(int i=0; i<numDec; i++){
            String str = String.valueOf(i);
			op = TestEntry.OP_TYPE.SETDATA.ordinal();
			TestEntry testEntry = new TestEntry(path, str, op);
			ClientRequest cr = new ClientRequest(client.hashCode(), i+200, testEntry.transformBytes());
			DecisionMessage msg = new DecisionMessage(node, i+1, cr.toRequestByteArray());
			//DecisionMessage msg = new DecisionMessage(node, client.hashCode(), i+200, i+1, testEntry.transformBytes());
			log.update(msg);
			//System.out.println("append "+msg);
		}
		//for(int i=1000; i>=990; i--){
        //    String str = String.valueOf(i);
		//	op = TestEntry.OP_TYPE.GETDATA.ordinal();
		//	TestEntry testEntry = new TestEntry(path, str, op);
		//	DecisionMessage msg = new DecisionMessage(node, client.hashCode()+1, i+500, i, testEntry.transformBytes());
		//	log.update(msg);
		//	System.out.println("append "+msg);
		//}
		//log.sync();
		log.garbageCollect(50000);
		//log.sync();

		/*
		Iterator<DecisionMessage> iter = null;
		DecisionMessage decision;
		long start;
		long itPos = 0;
		for (start=65001; start<65020; start+=10) {
			long end = start + 10 - 1;
			long index = 0;

			if (itPos>=start || null == iter) {
				iter = log.iterator();
				itPos = start;
				System.out.printf("reInit log iterator\n");
			}
			System.out.printf("=>itPos=%d start %d\n", itPos, start);

			while (iter.hasNext()) {
				decision = iter.next();
				itPos++;
				index = decision.getIndex();
				if (index >= start) {
					System.out.printf("p it.%d => %s\n", itPos, decision);
				}
				if (index >= end) {
					itPos = end;
					System.out.printf("break index %d >= end %d\n", index, end);
					break;
				}
			}
		}*/
		log.readLogChunk(50001);
		log.garbageCollect(numDec);
		Thread.sleep(5000);
		log.close();
		//log = null;
		System.out.printf("finish batch process decision.%d\n", numDec);
	}
}
