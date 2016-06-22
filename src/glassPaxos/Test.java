package glassPaxos;

import java.net.InetSocketAddress;

import glassPaxos.network.EventHandler;
import glassPaxos.network.NettyNetwork;
import glassPaxos.network.Network;
import glassPaxos.network.NodeIdentifier;
import glassPaxos.network.SimpleNetwork;
import glassPaxos.network.messages.Message;
import glassPaxos.network.messages.TestMessage;

public class Test {
	public static void main(String []args) throws Exception{
		NodeIdentifier id0 = new NodeIdentifier(NodeIdentifier.Role.CLIENT,  0);
		NodeIdentifier id1 = new NodeIdentifier(NodeIdentifier.Role.CLIENT,  1);
		Configuration.addActiveLogger("NettyNetwork", SimpleLogger.INFO);
		Configuration.addNodeAddress(id0,
									 new InetSocketAddress("localhost", 5002));
		Configuration.addNodeAddress(id1,
				 new InetSocketAddress("localhost", 5003));
		TestClient client0 = new TestClient(id0);
		TestClient client1 = new TestClient(id1);
		client0.sendValue(id1, 5353);
		//client1.sendValue(id0, 5354);
	}
	
	
	public static class TestClient implements EventHandler{
		NodeIdentifier myID = null;
		Network network = null;
		public TestClient(NodeIdentifier myID){
			this.myID = myID;
			network = new NettyNetwork(myID, this);
		}
		
		public void sendValue(NodeIdentifier receiver, int value){
			network.sendMessage(receiver, new TestMessage(myID, value));
		}
		
		@Override
		public void handleMessage(Message msg) {
			System.out.println(msg);
			network.sendMessage(msg.getSender(), new TestMessage(myID, ((TestMessage)msg).getValue()+1));
			
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
