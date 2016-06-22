package glassPaxos.interfaces;

import java.util.List;

public interface AppServerInterface {
	public byte[] executeRequest(Request req);
	
	public void takeSnapshot(long index);
	
	public void loadSnapshot(long index);
	
	public List<byte[]> getSnapshotTokens(long index);

	public long getLatestSnapshotIndex();
	
	public byte[] getSnapshotChunk(byte[] token);
	
	public void storeSnapshotChunk(byte[] token, byte[] data);
	
	
}
