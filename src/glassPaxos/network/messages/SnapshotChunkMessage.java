package glassPaxos.network.messages;

import glassPaxos.network.NodeIdentifier;
import io.netty.buffer.ByteBuf;

public class SnapshotChunkMessage extends Message {

	/* set numOfChunks to 1 */
	private long checkpointIndex;
	private int chunkOffset; //start from 1
	private int totalChunks;
    private byte []snapshotToken;
	private byte []snapshotChunk;

	protected SnapshotChunkMessage(){}

	public SnapshotChunkMessage(NodeIdentifier sender, long idx, int offset, int num, byte []token, byte []chunk) {
		super(MSG_TYPE.SNAPSHOTCHUNK, sender);
		this.checkpointIndex = idx;
		this.chunkOffset = offset;
		this.totalChunks = num;
		this.snapshotToken = token;
        this.snapshotChunk= chunk;
	}

	public long getCheckpointIndex() {
		return checkpointIndex;
	}

	public int getChunkOffset() {
		return chunkOffset;
	}

	public int getTotalChunks() {
		return totalChunks;
	}

	public byte[] getSnapshotToken() {
		return snapshotToken;
	}

	public byte[] getSnapshotChunk() {
		return snapshotChunk;
	}

	@Override
	public void serialize(ByteBuf buf){
		super.serialize(buf);
		buf.writeLong(checkpointIndex);
		buf.writeInt(chunkOffset);
		buf.writeInt(totalChunks);
        if (snapshotToken != null) {
		    buf.writeInt(snapshotToken.length);
		    buf.writeBytes(snapshotToken);
        } else {
            buf.writeInt(0);
        }
        if (snapshotChunk != null) {
		    buf.writeInt(snapshotChunk.length);
		    buf.writeBytes(snapshotChunk);
        } else {
            buf.writeInt(0);
        }
	}
	
	@Override
	public void deserialize(ByteBuf buf) {
		super.deserialize(buf);
		checkpointIndex = buf.readLong();
		chunkOffset = buf.readInt();
		totalChunks = buf.readInt();
		int len = buf.readInt();
        if (len > 0) {
		    snapshotToken = new byte[len];
		    buf.readBytes(snapshotToken, 0, len);
        } else {
            snapshotToken = null;
        }
		len = buf.readInt();
        if (len > 0) {
		    snapshotChunk = new byte[len];
		    buf.readBytes(snapshotChunk, 0, len);
        } else {
            snapshotChunk = null;
        }
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("SnapshotChunkMessage<src ").append(super.getSender())
		.append(", checkpoint ").append(checkpointIndex)
		.append(", [").append(chunkOffset).append("/").append(totalChunks).append("]");
        if (snapshotToken != null)
            sb.append(", tokensLen ").append(snapshotToken.length);
        if (snapshotChunk != null)
            sb.append(", chunksLen ").append(snapshotChunk.length);
        sb.append(">");
		return sb.toString();
	}

}
