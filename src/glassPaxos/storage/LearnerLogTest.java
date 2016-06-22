package glassPaxos.storage;

import glassPaxos.Configuration;
import glassPaxos.SimpleLogger;
import glassPaxos.learner.Learner;
import glassPaxos.network.NodeIdentifier;
import glassPaxos.network.messages.ClientRequest;
import glassPaxos.network.messages.DecisionMessage;

import java.io.File;
import java.util.Iterator;

public class LearnerLogTest {
	public static void main(String[] args) throws Exception{
		Configuration.addActiveLogger("LearnerLog", SimpleLogger.INFO);
		if(args.length != 3){
			System.err.println("java LearnerLogTest <logDir> <time> <reqSize>");
			return;
		}
		String logDirName = args[0];
		long time = Long.parseLong(args[1]);
		int reqSize = Integer.parseInt(args[2]);
		File logDir = new File(logDirName);
		for(File f: logDir.listFiles()){
			f.delete();
		}
		Learner learner = new Learner();
		LearnerLog log = new LearnerLog(logDirName, learner);
		NodeIdentifier node = new NodeIdentifier(NodeIdentifier.Role.ACCEPTOR, 1);
		NodeIdentifier client = new NodeIdentifier(NodeIdentifier.Role.CLIENT, 2);
		
		
		long requestCount = 0;
		long startTime = System.currentTimeMillis();
		byte[] request = new byte[reqSize];
		int i = 0;
		ClientRequest cr;
		while(System.currentTimeMillis() - startTime < time){
			cr = new ClientRequest(client.hashCode(), i+100, request);
			DecisionMessage msg = new DecisionMessage(node, i, cr.toRequestByteArray());
			//DecisionMessage msg = new DecisionMessage(node, client.hashCode(), i+100, i, request);
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
		log = new LearnerLog(logDirName, learner);
		Iterator<DecisionMessage> iter = log.iterator(-1);
		requestCount = 0;
		while(iter.hasNext()){
			DecisionMessage msg = iter.next();
			requestCount ++;
		}
		System.out.println("read throughput = "+ (requestCount * 1000L / (System.currentTimeMillis() - startTime)));
		log.close();
	}
}
