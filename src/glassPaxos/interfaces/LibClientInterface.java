package glassPaxos.interfaces;

/**
 * This is the interface our Paxos client provides to the App client
 * @author yangwang
 *
 */
public interface LibClientInterface {
	
	/*
	 * The App client uses this function to send a request to the
	 * App server. The App Client should provide a callback function
	 * to the Paxos client so that when the request is completed,
	 * the Paxos client can notify the App client.
	 */
	public void sendRequest(byte[] req, AppClientCallback callback);
}
