package glassPaxos.network;

import glassPaxos.network.messages.Message;

import java.io.IOException;

/*
 * The networking layer provides an unreliable but best-effort service to
 * send and receive messages.
 * 
 * Messages may be lost for various reasons, and it is
 * the upper layer's responsibility to detect those losses and 
 * retransmit messages when necessary.
 */
public abstract class Network {
	
	protected NodeIdentifier myID;
	protected EventHandler eventHandler;
	
	public Network(NodeIdentifier myID, EventHandler eventHandler){
		this.myID = myID;
		this.eventHandler = eventHandler;
	}
	
	public abstract void sendMessage(NodeIdentifier receiver, Message msg);

	public abstract void localSendMessage(Message msg);

}
