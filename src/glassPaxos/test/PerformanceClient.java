package glassPaxos.test;

import glassPaxos.Configuration;
import glassPaxos.SimpleLogger;
import glassPaxos.Test.TestClient;
import glassPaxos.network.EventHandler;
import glassPaxos.network.NettyNetwork;
import glassPaxos.network.Network;
import glassPaxos.network.NodeIdentifier;
import glassPaxos.network.SimpleNetwork;
import glassPaxos.network.messages.Message;
import glassPaxos.network.messages.TestMessage;

import java.net.InetSocketAddress;

public class PerformanceClient {
	
	public static void main(String []args) throws Exception{
		NodeIdentifier clientID = new NodeIdentifier(NodeIdentifier.Role.CLIENT,  0);
		NodeIdentifier serverID = new NodeIdentifier(NodeIdentifier.Role.LEARNER,  0);
		Configuration.addActiveLogger("NettyNetwork", SimpleLogger.INFO);
		Configuration.addNodeAddress(clientID,
									 new InetSocketAddress("node730-1", 5000));
		Configuration.addNodeAddress(serverID,
				 new InetSocketAddress("node730-2", 5000));
		TestClient client = new TestClient(clientID);
		long start = System.currentTimeMillis();
		long realstart = System.currentTimeMillis();
		long count = 0;
		long realCount = 0;
		while(true){
			for(int i=0; i<5; i++){
				client.send(serverID, (int)count);
				count ++;
				realCount ++;
			}
			if(System.currentTimeMillis() - start > 1000){
				System.out.println("thoughput="+(count*1000L)/(System.currentTimeMillis()-start)+" time="+(System.currentTimeMillis() - start));
				System.out.println("realthoughput="+(realCount*1000L)/(System.currentTimeMillis()-realstart)+" time="+(System.currentTimeMillis() - realstart));
				start = System.currentTimeMillis();
				count = 0;
			}
		}
	}
	
	
	public static class TestClient implements EventHandler{
		NodeIdentifier myID = null;
		Network network = null;
		public TestClient(NodeIdentifier myID){
			this.myID = myID;
			network = new NettyNetwork(myID, this);
		}
		
		public void send(NodeIdentifier server, int val){
			network.sendMessage(server, new TestMessage(myID, val));
		}
		
		@Override
		public void handleMessage(Message msg) {
			
		}

		@Override
		public void handleTimer() {
			
			
		}

		@Override
		public void handleFailure(NodeIdentifier node, Throwable cause) {
			// TODO Auto-generated method stub
			
		}
		
	}
}
