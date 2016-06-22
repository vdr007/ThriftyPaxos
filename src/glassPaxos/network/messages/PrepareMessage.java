package glassPaxos.network.messages;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import glassPaxos.network.NodeIdentifier;

public class PrepareMessage extends Message {
	
	private int round;
	private long checkpoint;
	private long firstUnstable;

	protected PrepareMessage(){}
	
	public PrepareMessage(NodeIdentifier sender, int round, long checkpoint, long firstUnstable) {
		super(Message.MSG_TYPE.PREPARE, sender);
		this.round = round;
		this.checkpoint = checkpoint;
		this.firstUnstable = firstUnstable;
	}

	public int getRound() {
		return round;
	}

	public long getCheckpoint() {
		return checkpoint;
	}

	public long getFirstUnstable() {
		return firstUnstable;
	}
	
	@Override
	public void serialize(ByteBuf buf){
		super.serialize(buf);
		buf.writeInt(round);
		buf.writeLong(checkpoint);
		buf.writeLong(firstUnstable);
	}
	
	@Override
	public void deserialize(ByteBuf buf){
		super.deserialize(buf);
        round = buf.readInt();
        checkpoint = buf.readLong();
		firstUnstable = buf.readLong();
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("PrepareMessage <src ").append(super.getSender())
				.append(", round ").append(round)
				.append(", ckPt ").append(checkpoint)
				.append(", firstUn ").append(firstUnstable)
				.append(">");
		return sb.toString();
	}

}
