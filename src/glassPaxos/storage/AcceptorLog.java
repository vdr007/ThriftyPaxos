package glassPaxos.storage;

import glassPaxos.Configuration;
import glassPaxos.SimpleLogger;
import glassPaxos.client.TestEntry;
import glassPaxos.interfaces.AcceptorLogCbInterface;
import glassPaxos.network.NodeIdentifier;
import glassPaxos.network.messages.AcceptMessage;
import glassPaxos.network.messages.ClientRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class AcceptorLog {//implements Iterable<AcceptMessage> {
	private SimpleLogger LOG = SimpleLogger.getLogger("AcceptorLog");
	private final String path;
	private static final long MAX_LOG_SIZE = 1048576L * 200L; //50L;
	
	private SequentialLog log = null;
	private AcceptMessage lastMessage = null;
	private GarbageCollectThread gcThread = new GarbageCollectThread();
	private AcceptorLogCbInterface accLogCbInterface;

	public Iterator<AcceptMessage> iterator(long startIndex) {
        return new AcceptorLogIterator(startIndex);
    }
	
	public AcceptorLog(String path, AcceptorLogCbInterface cb){
		this.path = path;
		this.accLogCbInterface = cb;
		gcThread.start();
	}
	
	/*
	 * If a log entry is out of order, AcceptorLog will create a new
	 * sequential log. Therefore, too many out-of-order log can hurt
	 * performance.
	 */
	private void createNewSequentialLogWhenNecessary(AcceptMessage msg){
		if(log == null || 
				(lastMessage!=null && (msg.getIndex() < lastMessage.getIndex() ||
						msg.getRound() > lastMessage.getRound()))){
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
	public void update(AcceptMessage msg){
		createNewSequentialLogWhenNecessary(msg);
		log.append(msg);
		lastMessage = msg;
	}
	
	/*
	 * Sync data to disk.
	 */
	//public void doSync(long index){
	public void sync(){
		long ts = System.nanoTime();
		if(log != null)
			log.sync();
		//System.out.printf("doSync=%d time=%.1f s\n", index, (System.nanoTime()-ts)/1000000000.0);
	}

	public void moveLog() {
		gcThread.addTask(-2);
		System.out.printf("accLog add moveLog task\n");
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
		gcThread.addTask(index);
	}

	//public void sync() {
	//	gcThread.addTask(-1); // -1 represents sync()
	//}

	private void doMoveLog(long index){
		if (accLogCbInterface != null) {
			accLogCbInterface.backgroundTask(index);
		} else {
			System.out.printf("error call doMoveLog() accLogCbInterface=%s\n", accLogCbInterface);
		}
	}

	private void doGarbageCollect(long index){
		File[] seqLogDirs = new File(path).listFiles();
		for(int i=0; i<seqLogDirs.length; i++){
			SequentialLog tmp = new SequentialLog(seqLogDirs[i].getPath());
			tmp.garbageCollect(index);
		}
		System.out.printf("doGarbageCollect=%d\n", index);
	}
	
	private class GarbageCollectThread extends Thread{
		private LinkedBlockingQueue<Long> queue = new LinkedBlockingQueue<Long>(100);
		private boolean running = true;
		
		public void addTask(long index){
			try{
				queue.put(index);
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
					long index = queue.take();
					////doGarbageCollect(index);

					if (index == -2) {
						//doSync(index);
						doMoveLog(index);
					} else {
						doGarbageCollect(index);
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
	private class AcceptorLogIterator implements Iterator<AcceptMessage> {

		private Iterator<AcceptMessage> [] iterators = null;
		private AcceptMessage[] heads = null;
		
		public AcceptorLogIterator(long startIndex) {
			File[] seqLogDirs = new File(path).listFiles();
			iterators = new Iterator[seqLogDirs.length];
			heads = new AcceptMessage[seqLogDirs.length];
			for(int i=0; i<iterators.length; i++){
				iterators[i] = new SequentialLog(seqLogDirs[i].getPath()).iterator(startIndex);
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
			
		public AcceptMessage next() {
			if(this.hasNext()) {
				//do a merge sort on multiple sequential logs
			    AcceptMessage ret = null;
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
			    	else if(ret.getIndex() == heads[i].getIndex() && ret.getRound() > heads[i].getRound()){
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

	
	private class SequentialLog {//implements Iterable<AcceptMessage>{
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
		
		public Iterator<AcceptMessage> iterator(long startIndex) {
	        return new SequentialLogIterator(startIndex);
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
		
		/**
		 * List files starting from startIndex (included)
		 * @param startIndex
		 * @return
		 */
		private File[] listFiles(long startIndex){
			File f = new File(path);
			File [] tmp = f.listFiles();
			Arrays.sort(tmp, new Comparator<File>() {
	            @Override
	            public int compare(File o1, File o2) {
	                long n1 = extractNumber(o1.getName());
	                long n2 = extractNumber(o2.getName());
	                return (int)(n1 - n2);
	            }

	            
	        });
			ArrayList<File> ret = new ArrayList<File>();
			for(int i=0; i<tmp.length; i++){
				if(i+1 < tmp.length && extractNumber(tmp[i+1].getName()) < startIndex)
					continue;
				ret.add(tmp[i]);
			}
			File[] retArray = new File[ret.size()];
			ret.toArray(retArray);
			return retArray;
		}
		
		public void garbageCollect(long index){
			LOG.info("garbage collect "+index+"\n");
			File [] logs = listFiles(-1);
			int lastLogToDelete = -1;
			for(int i=0; i<logs.length; i++){
				File f = logs[i];
				long startIndex = extractNumber(f.getName());
				if(startIndex <= index){
					lastLogToDelete = i - 1;
				}
			}
			for(int i=0; i<=lastLogToDelete; i++){
				LOG.info("Garbage collect %s\n", logs[i].getPath());
				logs[i].delete();
			}
		}
		
		private void createNewLogWhenNecessary(long index) {
			try{
				//if(current!=null)
				//	current.flush();
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
		
		public void append(AcceptMessage msg){
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
		
		private class SequentialLogIterator implements Iterator<AcceptMessage> {

			private File[] logs = null;
			private int currentLog = 0;
			private DataInputStream dis = null;
			private AcceptMessage next = null;
			private final byte[] bytes = new byte[Configuration.MAX_MSG_SIZE];
			private final ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);
			
			public SequentialLogIterator(long startIndex) {
				this.logs = listFiles(startIndex);
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
						AcceptMessage msg = new AcceptMessage();
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
				
			public AcceptMessage next() {
				if(this.hasNext()) {
				    AcceptMessage ret = next;
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

	private static void basicTest() {
		//todo
		AcceptorLog log = new AcceptorLog("/Users/yangwang/logtest", null);
		NodeIdentifier node = new NodeIdentifier(NodeIdentifier.Role.ACCEPTOR, 1);
		NodeIdentifier client = new NodeIdentifier(NodeIdentifier.Role.CLIENT, 2);
		int op, sLen;
		String path = "AcceptorLogTest";
		ClientRequest cr;
		for(int i=0; i<1000; i++){
			String str = String.valueOf(i);
			op = TestEntry.OP_TYPE.SETDATA.ordinal();
//            sLen = str.getBytes("UTF-8").length;
			TestEntry testEntry = new TestEntry(path, str, op);
//			AcceptMessage msg = new AcceptMessage(node, 0, i, 0, op, sLen, str);
			//AcceptMessage msg = new AcceptMessage(node, client.hashCode(), i+100, 0, i, 0, testEntry.transformBytes());
			cr = new ClientRequest(client.hashCode(), i+100, testEntry.transformBytes());
			AcceptMessage msg = new AcceptMessage(node, 0, i, 0, cr.toRequestByteArray());
			log.update(msg);
			//System.out.println("append "+msg);
		}
		for(int i=1000; i>=990; i--){
			String str = String.valueOf(i);
			op = TestEntry.OP_TYPE.GETDATA.ordinal();
//            sLen = str.getBytes("UTF-8").length;
			TestEntry testEntry = new TestEntry(path, str, op);
			//AcceptMessage msg = new AcceptMessage(node, client.hashCode(), i+100, 0, i, 0, testEntry.transformBytes());
			cr = new ClientRequest(client.hashCode(), i+100, testEntry.transformBytes());
			AcceptMessage msg = new AcceptMessage(node, 0, i, 0, cr.toRequestByteArray());
			log.update(msg);
			//System.out.println("append "+msg);
		}
		for(int i=0; i<1000; i++){
			String str = String.valueOf(1000-i);
			op = TestEntry.OP_TYPE.SETDATA.ordinal();
//            sLen = str.getBytes("UTF-8").length;
			TestEntry testEntry = new TestEntry(path, str, op);
			//AcceptMessage msg = new AcceptMessage(node, client.hashCode(), i+100, 1000, i, 0, testEntry.transformBytes());
			cr = new ClientRequest(client.hashCode(), i+100, testEntry.transformBytes());
			AcceptMessage msg = new AcceptMessage(node, 1000, i, 0, cr.toRequestByteArray());
			log.update(msg);
			//System.out.println("append "+msg);
		}
		for(int i=0; i<1000; i++){
			String str = String.valueOf(i);
			op = TestEntry.OP_TYPE.GETDATA.ordinal();
//            sLen = str.getBytes("UTF-8").length;
			TestEntry testEntry = new TestEntry(path, str, op);
			//AcceptMessage msg = new AcceptMessage(node, client.hashCode(), i+100, 1, i, 0, testEntry.transformBytes());
			cr = new ClientRequest(client.hashCode(), i+100, testEntry.transformBytes());
			AcceptMessage msg = new AcceptMessage(node, 1, i, 0, cr.toRequestByteArray());
			log.update(msg);
			//System.out.println("append "+msg);
		}
		log.sync();
		log.garbageCollect(990);
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		Iterator<AcceptMessage> iter = log.iterator(-1);
		while(iter.hasNext()){
			System.out.println(iter.next());
		}
	}

	private static void moveDirectoryTestA(String oPath, String nPath, int logSize) {
		AcceptorLog oldLog = new AcceptorLog(oPath, null);
		AcceptorLog newLog = new AcceptorLog(nPath, null);
		NodeIdentifier acceptor = new NodeIdentifier(NodeIdentifier.Role.ACCEPTOR, 1);
		NodeIdentifier client = new NodeIdentifier(NodeIdentifier.Role.CLIENT, 2);
		int op = TestEntry.OP_TYPE.SETDATA.ordinal();
		String path = "AcceptorLogTest";
		AcceptMessage accept;
		int count = logSize;
		int offset = 2*count;
		ClientRequest cr;
		for(int i=0; i<count; i++){
			String str = String.valueOf(i);
			TestEntry testEntry = new TestEntry(path, str, op);
			//accept = new AcceptMessage(acceptor, client.hashCode(), i+1, 0, i, 0, testEntry.transformBytes());
			cr = new ClientRequest(client.hashCode(), i+1, testEntry.transformBytes());
			accept = new AcceptMessage(acceptor, 0, i, 0, cr.toRequestByteArray());
			oldLog.update(accept);
			//accept = new AcceptMessage(acceptor, client.hashCode(), i+10000+offset, 0, i+offset, 0, testEntry.transformBytes());
			cr = new ClientRequest(client.hashCode(), i+1000+offset, testEntry.transformBytes());
			accept = new AcceptMessage(acceptor, 0, i+offset, 0, cr.toRequestByteArray());
			newLog.update(accept);
		}
		oldLog.sync();
		newLog.sync();
		oldLog.close();
		newLog.close();
		System.out.printf("finish create old and new logs\n");
	}

	public void moveDirectory(String oPath, String nPath) {
		long ts = System.currentTimeMillis();
		Path srcPath = Paths.get(oPath);
		Path dstPath = Paths.get(nPath);
		try {
			if (!Files.exists(dstPath)) {
				Files.createDirectory(dstPath);
				System.out.printf("FV create new dir %s\n", nPath);
			}

			Files.walkFileTree(srcPath, new FileVisitor(srcPath, dstPath));

			if (!Files.exists(srcPath)) {
				Files.createDirectory(srcPath);
				System.out.printf("FV create old dir %s\n", oPath);
			}
			System.out.printf("FV finish moveDirectory time=%.2f s\n", (System.currentTimeMillis()-ts)/1000.0);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static class FileVisitor extends SimpleFileVisitor<Path> {
		private Path src;
		private Path dst;
		double fileSize = 0.0;
		public FileVisitor(Path oPath, Path nPath) {
			this.src = oPath;
			this.dst = nPath;
		}

		@Override
		public FileVisitResult preVisitDirectory (Path dir, BasicFileAttributes attrs)throws IOException {
			Path targetPath = dst.resolve(src.relativize(dir));
			if (!Files.exists(targetPath)) {
				Files.createDirectory(targetPath);
			}
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult visitFile (Path file, BasicFileAttributes attrs)throws IOException {
			fileSize += attrs.size()/(1024.0*1024);
			System.out.printf("FV cp and remove file %s, %.1f, total=%.1f MB\n", file.toString(),
					attrs.size()/(1024*1024.0), fileSize);
			Files.copy(file, dst.resolve(src.relativize(file)), StandardCopyOption.REPLACE_EXISTING);
			Files.delete(file);
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult postVisitDirectory (Path dir, IOException exc)throws IOException {
			System.out.printf("FV delete dir %s\n", dir.toString());
			Files.delete(dir);
			return FileVisitResult.CONTINUE;
		}
	}

	private static boolean deleteDir(File dir) {
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i=0; i<children.length; i++) {
				boolean success = deleteDir(new File(dir, children[i]));
				if (!success) {
					return false;
				}
			}
		}
		System.out.printf("delete %s\n", dir.getAbsolutePath());
		return dir.delete();
	}

	private static void scanNewLog(String path) {
		AcceptorLog log = new AcceptorLog(path, null);
		Iterator<AcceptMessage> it = log.iterator(-1);
		while(it.hasNext()){
			System.out.println(it.next());
		}
		log.close();
	}

	public static void main(String []args) {
		Configuration.addActiveLogger("AcceptorLog", SimpleLogger.DEBUG);
		//File logDir = new File("/Users/rongshi/ywlab/glassPaxos/logtest");
		// for(File f: logDir.listFiles()){
		// 	f.delete();
		// }
		// basicTest();

		// if (logDir.isDirectory()) {
		// 	String[] children = logDir.list();
		// 	for (int k=0; k<children.length; k++) {
		// 		boolean successClean = deleteDir(logDir);
		// 		System.out.printf("=>delete dir=%s, success=%b\n", logDir.getAbsolutePath(), successClean);
		// 	}
		// }
		//String oldPath = "/Users/rongshi/ywlab/glassPaxos/logtest/oldLog";
		//String newPath = "/Users/rongshi/ywlab/glassPaxos/logtest/newLog";
		String oldPath = "/home/hadoop-shir/catchupacceptorlog";
		String newPath = "/home/hadoop-shir/acceptorlog";
		// try {
		// 	if (!Files.exists(Paths.get(oldPath))) {
		// 		Files.createDirectory(Paths.get(oldPath));
		// 		System.out.printf("create old dir %s\n", oldPath);
		// 	}
		// 	if (!Files.exists(Paths.get(newPath))) {
		// 		Files.createDirectory(Paths.get(newPath));
		// 		System.out.printf("create new dir %s\n", newPath);
		// 	}
		// } catch (IOException e) {
		// 	e.printStackTrace();
		// }
		// moveDirectoryTestA(oldPath, newPath, 10);
		// scanNewLog(oldPath);
		// scanNewLog(newPath);

		//moveDirectory(oldPath, newPath);
		//scanNewLog(newPath);

		// try {
		// 	Thread.sleep(2000);
		// } catch (InterruptedException e) {
		// 	e.printStackTrace();
		// }
	}
}
