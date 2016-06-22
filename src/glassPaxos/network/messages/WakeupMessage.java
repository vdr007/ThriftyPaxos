package glassPaxos.network.messages;
import glassPaxos.network.NodeIdentifier;
import io.netty.buffer.ByteBuf;

public class WakeupMessage extends Message {

	private long checkpoint;
	private long maxLogIndex;

	protected WakeupMessage(){}
	
	public WakeupMessage(NodeIdentifier sender, long checkpoint, long maxLogIndex) {
		super(Message.MSG_TYPE.WAKEUP, sender);
		this.checkpoint = checkpoint;
		this.maxLogIndex = maxLogIndex;
	}

	public long getCheckpoint() {
		return checkpoint;
	}

	public long getMaxLogIndex() {
		return maxLogIndex;
	}

	@Override
	public void serialize(ByteBuf buf){
		super.serialize(buf);
		buf.writeLong(checkpoint);
		buf.writeLong(maxLogIndex);
	}
	
	@Override
	public void deserialize(ByteBuf buf){
		super.deserialize(buf);
        checkpoint = buf.readLong();
		maxLogIndex = buf.readLong();
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("WakeupMessage <src ").append(super.getSender())
			.append(", ckPt ").append(checkpoint)
			.append(", maxLog ").append(maxLogIndex).append(">");
		return sb.toString();
	}

}
