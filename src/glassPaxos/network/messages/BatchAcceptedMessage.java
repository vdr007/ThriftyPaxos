package glassPaxos.network.messages;

import glassPaxos.network.NodeIdentifier;
import io.netty.buffer.ByteBuf;

public class BatchAcceptedMessage extends Message {

	private int round;
	private long start;
	private long end;

	protected BatchAcceptedMessage(){}

	public BatchAcceptedMessage(NodeIdentifier sender, int round, long start, long end) {
		super(MSG_TYPE.BACCEPTED, sender);
		this.round = round;
		this.start = start;
		this.end = end;
	}
	
	public long getStart(){
		return start;
	}
	
	public long getEnd(){
		return end;
	}
	
	public int getRound(){
		return round;
	}
	
	@Override
	public void serialize(ByteBuf buf){
		super.serialize(buf);
		buf.writeInt(round);
		buf.writeLong(start);
		buf.writeLong(end);
	}
	
	@Override
	public void deserialize(ByteBuf buf){
		super.deserialize(buf);
        round = buf.readInt();
        start = buf.readLong();
        end = buf.readLong();
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("BatchAcceptedMessage <src ").append(super.getSender())
			.append(", round ").append(round).append(", start ").append(start)
			.append(", end ").append(end).append(">");

		return sb.toString();
	}
}
