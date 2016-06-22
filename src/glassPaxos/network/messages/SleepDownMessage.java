package glassPaxos.network.messages;

import glassPaxos.network.NodeIdentifier;
import io.netty.buffer.ByteBuf;

public class SleepDownMessage extends Message {

//	private long index;

	protected SleepDownMessage(){}

	public SleepDownMessage(NodeIdentifier sender) {
		super(MSG_TYPE.SLEEPDOWN, sender);
//		this.index = index;
	}

//	public long getIndex() {
//		return index;
//	}
	
	@Override
	public void serialize(ByteBuf buf){
		super.serialize(buf);
//		buf.writeLong(index);
	}
	
	@Override
	public void deserialize(ByteBuf buf){
		super.deserialize(buf);
//        index = buf.readLong();
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("SleepDownMessage <src ").append(super.getSender()).append(">");
		return sb.toString();
	}

}
