package glassPaxos.apps.hashtable;

import java.util.List;

import glassPaxos.Configuration;
import glassPaxos.SimpleLogger;
import glassPaxos.interfaces.AppClientCallback;
import glassPaxos.interfaces.LibClientInterface;
import glassPaxos.interfaces.LibServerInterface;
import glassPaxos.interfaces.Request;

public class Test {
	public static void main(String []args) throws Exception{
		Configuration.addActiveLogger("HashMapServer", SimpleLogger.DEBUG);
		Connector conn = new Connector();
		HashTableClient client = new HashTableClient(conn);
		HashTableServer server = new HashTableServer("/Users/yangwang/test");
		HashTableServer server2 = new HashTableServer("/Users/yangwang/test2");
		conn.setup(client, server);
		byte[] data = new byte[10000];
		for(int i=0; i<1000; i++){
			client.put(i, data, null);
		}
		server.takeSnapshot(1000);
		List<byte[]> tokens = server.getSnapshotTokens(1000);
		for(byte[] token : tokens){
			byte[] chunk = server.getSnapshotChunk(token);
			server2.storeSnapshotChunk(token, chunk);
		}
		server2.loadSnapshot(1000);
		conn.setup(client, server2);
		for(int i=0; i<1000; i++){
			client.get(i, null);
		}
	}
	
	public static class Connector implements LibClientInterface, LibServerInterface{

		private HashTableClient client;
		private HashTableServer server;
		
		private byte[] reply;
		
		public void setup(HashTableClient client, HashTableServer server){
			this.client = client;
			this.server = server;
		}
		
		@Override
		public void sendReply(Request req, byte[] reply) {
			this.reply = reply;
			
		}

		@Override
		public void sendRequest(byte[] req, AppClientCallback callback) {
			Request reqInfo = new Request();
			reqInfo.request = req;
			reqInfo.server = this;
			server.executeRequest(reqInfo);
			if(callback!=null)
				callback.requestComplete(req, true, reply);
		}

		@Override
		public void takeSnapshotComplete(long index) {
		}

		@Override
		public void loadSnapshotComplete(long index) {
		}
	}
}
