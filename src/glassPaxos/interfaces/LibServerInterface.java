package glassPaxos.interfaces;

public interface LibServerInterface {
	void sendReply(Request req, byte[] reply);
    void takeSnapshotComplete(long index);
    void loadSnapshotComplete(long index);
}
