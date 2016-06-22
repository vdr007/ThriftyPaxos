package glassPaxos.apps.hashtable;

import java.nio.ByteBuffer;

import glassPaxos.interfaces.AppClientCallback;
import glassPaxos.interfaces.LibClientInterface;

public class HashTableClient {
	private LibClientInterface paxosClient = null;
	
	public HashTableClient(LibClientInterface paxosClient){
		this.paxosClient = paxosClient;
	}
	
	public void put(long key, byte[] value, AppClientCallback callback){
		byte[] req = new byte[1 + 8 + value.length];
		ByteBuffer b = ByteBuffer.wrap(req);
		b.put((byte)1);
		b.putLong(key);
		b.put(value);
		paxosClient.sendRequest(req, callback);
	}
	
	public void get(long key, AppClientCallback callback){
		byte[] req = new byte[1 + 8];
		ByteBuffer b = ByteBuffer.wrap(req);
		b.put((byte)0);
		b.putLong(key);
		paxosClient.sendRequest(req, callback);
	}
}
