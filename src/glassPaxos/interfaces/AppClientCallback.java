package glassPaxos.interfaces;

/*
 * This is the callback function an App client provides to the Paxos client.
 * The Paxos client will use this callback to notify the App client when the
 * request is completed.
 */
public interface AppClientCallback {
	
	/*
	 * If completed==true, it means the server side has actually received
	 * the request and sent back a reply.
	 * 
	 * Note that "complete" is different from "succeed", which is determined
	 * by the application. For example, if the app is a remote hashtable and
	 * the client wants to read a non-existed key, the server may respond
	 * "Failed. Key does not exist". In this case, as long as the client receives
	 * the reply, the request is still considered "complete".
	 * 
	 * Incomplete request can be caused by network failures (the client cannot
	 * connect to the servers at all) or replica failures (more than f servers fail).
	 */
	public void requestComplete(byte[] req, boolean completed, byte[] reply);
}
