package glassPaxos.network.messages;

import glassPaxos.network.NodeIdentifier;
import io.netty.buffer.ByteBuf;

public class FakeLearnerSyncReplyMessage extends Message {

	private long checkpoint;
	private long firstUnstable;

	protected FakeLearnerSyncReplyMessage(){}

	public FakeLearnerSyncReplyMessage(NodeIdentifier sender, long check, long first) {
		super(MSG_TYPE.FAKELEARNERSYNC, sender);
		this.checkpoint = check;
		this.firstUnstable = first;
	}
	
	public long getCheckpoint(){
		return checkpoint;
	}

	public long getFirstUnstable() {
		return firstUnstable;
	}

	@Override
	public void serialize(ByteBuf buf){
		super.serialize(buf);
		buf.writeLong(checkpoint);
		buf.writeLong(firstUnstable);
	}
	
	@Override
	public void deserialize(ByteBuf buf){
		super.deserialize(buf);
		checkpoint = buf.readLong();
		firstUnstable = buf.readLong();
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("FakeLearnerSyncReplyMessage <src ").append(super.getSender())
			.append(", checkpoint ").append(checkpoint)
			.append(", firstUnstable ").append(firstUnstable).append(">");
		return sb.toString();
	}

}
