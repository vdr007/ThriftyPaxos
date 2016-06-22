package glassPaxos.storage;

import glassPaxos.Configuration;
import glassPaxos.SimpleLogger;
import glassPaxos.client.TestEntry;
import glassPaxos.network.NodeIdentifier;
import glassPaxos.network.messages.AcceptMessage;
import glassPaxos.network.messages.ClientRequest;

import java.io.File;
import java.util.Iterator;

public class AcceptorLogTest {
	public static void main(String[] args) throws Exception{
		if(args.length != 3){
			System.err.println("java AcceptorTest <logDir> <time> <reqSize>");
			return;
		}
		String logDirName = args[0];
		long time = Long.parseLong(args[1]);
		int reqSize = Integer.parseInt(args[2]);
		File logDir = new File(logDirName);
		for(File f: logDir.listFiles()){
			f.delete();
		}
		AcceptorLog log = new AcceptorLog(logDirName, null);
		NodeIdentifier node = new NodeIdentifier(NodeIdentifier.Role.ACCEPTOR, 1);
		NodeIdentifier client = new NodeIdentifier(NodeIdentifier.Role.CLIENT, 2);
		
		
		long requestCount = 0;
		long startTime = System.currentTimeMillis();
		byte[] request = new byte[reqSize];
		int i = 0;
		ClientRequest cr;
		while(System.currentTimeMillis() - startTime < time){
			cr = new ClientRequest(client.hashCode(), i, request);
			AcceptMessage msg = new AcceptMessage(node, 0, i, 0, cr.toRequestByteArray());
			//AcceptMessage msg = new AcceptMessage(node, client.hashCode(), i, 0, i, 0, request);
			i++;
			log.update(msg);
			if(i%20000==19999)
				log.garbageCollect(i-19999);
			if(System.currentTimeMillis() - startTime > time/5)
				requestCount ++;
		}
		System.out.println("write throughput = "+ (requestCount * 5000L /4L / time));
		log.close();
		startTime = System.currentTimeMillis();
		log = new AcceptorLog(logDirName, null);
		Iterator<AcceptMessage> iter = log.iterator(-1);
		requestCount = 0;
		while(iter.hasNext()){
			AcceptMessage msg = iter.next();
			requestCount ++;
		}
		System.out.println("read throughput = "+ (requestCount * 1000L / (System.currentTimeMillis() - startTime)));
		log.close();
		/*int op, sLen;
		String path = "AcceptorLogTest";
		for(int i=0; i<1000; i++){
            String str = String.valueOf(i);
			op = TestEntry.OP_TYPE.SETDATA.ordinal();
//            sLen = str.getBytes("UTF-8").length;
			TestEntry testEntry = new TestEntry(path, str, op);
//			AcceptMessage msg = new AcceptMessage(node, 0, i, 0, op, sLen, str);
			AcceptMessage msg = new AcceptMessage(node, client.hashCode(), i, 0, i, 0, testEntry.transformBytes());
			log.update(msg);
			//System.out.println("append "+msg);
		}
		for(int i=1000; i>=990; i--){
            String str = String.valueOf(i);
			op = TestEntry.OP_TYPE.GETDATA.ordinal();
//            sLen = str.getBytes("UTF-8").length;
			TestEntry testEntry = new TestEntry(path, str, op);
			AcceptMessage msg = new AcceptMessage(node, client.hashCode(), i+100, 0, i, 0, testEntry.transformBytes());
			log.update(msg);
			//System.out.println("append "+msg);
		}
		for(int i=0; i<1000; i++){
            String str = String.valueOf(1000-i);
			op = TestEntry.OP_TYPE.SETDATA.ordinal();
//            sLen = str.getBytes("UTF-8").length;
			TestEntry testEntry = new TestEntry(path, str, op);
			AcceptMessage msg = new AcceptMessage(node, client.hashCode(), i+100, 1000, i, 0, testEntry.transformBytes());
			log.update(msg);
			//System.out.println("append "+msg);
		}
		for(int i=0; i<1000; i++){
            String str = String.valueOf(i);
			op = TestEntry.OP_TYPE.GETDATA.ordinal();
//            sLen = str.getBytes("UTF-8").length;
			TestEntry testEntry = new TestEntry(path, str, op);
			AcceptMessage msg = new AcceptMessage(node, client.hashCode(), i+100, 1, i, 0, testEntry.transformBytes());
			log.update(msg);
			//System.out.println("append "+msg);
		}
		log.sync();
		log.garbageCollect(990);
		Iterator<AcceptMessage> iter = log.iterator();
		while(iter.hasNext()){
			System.out.println(iter.next());
		}*/
	}
}
