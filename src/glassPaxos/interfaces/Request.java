package glassPaxos.interfaces;

import glassPaxos.network.NodeIdentifier;

public class Request {
	public byte[] request;
	public NodeIdentifier client; 
	public long clientSeqID;
	public LibServerInterface server;
}
