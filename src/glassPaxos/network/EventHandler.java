package glassPaxos.network;

import glassPaxos.network.messages.Message;

public interface EventHandler {
	/*
	 * Handle a message from another node
	 */
	public void handleMessage(Message msg);
	
	/*
	 * Handle a timer event. A timer event is triggered if
	 * there is no other event in a given amount of time (100ms).
	 */
	public void handleTimer();
	
	/*
	 * Handle a failure event. A failure event is triggered
	 * if the corresponding connection is broken.
	 */
	public void handleFailure(NodeIdentifier node, Throwable cause);
}
