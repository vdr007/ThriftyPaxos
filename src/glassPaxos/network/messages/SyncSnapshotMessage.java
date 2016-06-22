package glassPaxos.network.messages;

import glassPaxos.network.NodeIdentifier;
import io.netty.buffer.ByteBuf;

public class SyncSnapshotMessage extends Message {

	private long checkpoint;
	private int nextChunk;
	private int unstable; //first unstable offset
	private int numChunks; // sender: totalChunks, receiver: numberOfSyncChunks

	protected SyncSnapshotMessage(){}

	public SyncSnapshotMessage(NodeIdentifier sender, long checkpoint, int nextChunk, int unstable, int numChunks) {
		super(MSG_TYPE.SYNCSNAPSHOT, sender);
		this.checkpoint = checkpoint;
		this.nextChunk = nextChunk;
		this.unstable = unstable;
		this.numChunks = numChunks;
	}

	public long getCheckpoint() {
		return checkpoint;
	}

	public int getUnstable() {
		return unstable;
	}

	public int getNextChunk() {
		return nextChunk;
	}

	public int getNumChunks() {
		return numChunks;
	}

	@Override
	public void serialize(ByteBuf buf){
		super.serialize(buf);
		buf.writeLong(checkpoint);
		buf.writeInt(nextChunk);
		buf.writeInt(unstable);
		buf.writeInt(numChunks);
	}

	@Override
	public void deserialize(ByteBuf buf){
		super.deserialize(buf);
        checkpoint = buf.readLong();
		nextChunk = buf.readInt();
		unstable = buf.readInt();
		numChunks = buf.readInt();
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("SyncSnapshotMessage<src ").append(super.getSender())
			.append(", checkpoint ").append(checkpoint)
			.append(", nextChunk ").append(nextChunk)
			.append(", unstable ").append(unstable)
			.append(", numChunks ").append(numChunks)
			.append(">");
		return sb.toString();
	}

}
