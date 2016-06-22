package glassPaxos.network.messages;

import glassPaxos.network.NodeIdentifier;
import io.netty.buffer.ByteBuf;

public class AcceptedMessage extends Message {
	
	private int round;
	private long index;
	//private long first_unstable_index;

	protected AcceptedMessage(){}
	
	public AcceptedMessage(NodeIdentifier sender, int round, long index /*, long first_unstable_index*/) {
		super(Message.MSG_TYPE.ACCEPTED, sender);
		this.round = round;
		this.index = index;
		//this.first_unstable_index = first_unstable_index;
	}
	
	public long getIndex(){
		return index;
	}
	
	//public long getUnstblIdx(){
	//	return first_unstable_index;
	//}
	
	public int getRound(){
		return round;
	}
	
	@Override
	public void serialize(ByteBuf buf){
		super.serialize(buf);
		buf.writeInt(round);
		buf.writeLong(index);
		//buf.writeLong(first_unstable_index);
	}
	
	@Override
	public void deserialize(ByteBuf buf){
		super.deserialize(buf);
        round = buf.readInt();
        index = buf.readLong();
        //first_unstable_index = buf.readLong();
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("AcceptedMessage <src ").append(super.getSender())
			.append(", round ").append(round).append(", index ").append(index)
			//.append(", firstUnstable ").append(first_unstable_index)
			.append(">");

		return sb.toString();
	}
}
