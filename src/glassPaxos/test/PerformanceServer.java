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

public class PerformanceServer {
	
	public static void main(String []args) throws Exception{
		NodeIdentifier clientID = new NodeIdentifier(NodeIdentifier.Role.CLIENT,  0);
		NodeIdentifier serverID = new NodeIdentifier(NodeIdentifier.Role.LEARNER,  0);
		Configuration.addActiveLogger("NettyNetwork", SimpleLogger.INFO);
		Configuration.addNodeAddress(clientID,
									 new InetSocketAddress("node730-1", 5000));
		Configuration.addNodeAddress(serverID,
				 new InetSocketAddress("node730-2", 5000));
		TestServer server = new TestServer(serverID);
	}
	
	
	public static class TestServer implements EventHandler{
		NodeIdentifier myID = null;
		Network network = null;
		public TestServer(NodeIdentifier myID){
			this.myID = myID;
			network = new NettyNetwork(myID, this);
		}
		
		@Override
		public void handleMessage(Message msg) {
			//System.out.println(msg);

		}

		@Override
		public void handleTimer() {
			//System.out.println("timer event");
			
		}

		@Override
		public void handleFailure(NodeIdentifier node, Throwable cause) {
			// TODO Auto-generated method stub
			
		}
		
	}
}
