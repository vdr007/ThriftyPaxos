package glassPaxos.network.messages;

import glassPaxos.network.NodeIdentifier;
import io.netty.buffer.ByteBuf;

public class FreeLogMessage extends Message {

	private long index;
	//learner will send "0", leader will send swapOutLearner
	private int swapOutLearner;

	protected FreeLogMessage(){}

	public FreeLogMessage(NodeIdentifier sender, long index, int swapOutLearner) {
		super(MSG_TYPE.FREELOG, sender);
		this.index = index;
		this.swapOutLearner = swapOutLearner;
	}
	
	public long getIndex(){
		return index;
	}

	public int getSwapOutLearner() {
		return swapOutLearner;
	}
	
	@Override
	public void serialize(ByteBuf buf){
		super.serialize(buf);
		buf.writeLong(index);
		buf.writeInt(swapOutLearner);
	}
	
	@Override
	public void deserialize(ByteBuf buf){
		super.deserialize(buf);
		index = buf.readLong();
		swapOutLearner = buf.readInt();
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("FreeLogMessage <src ").append(super.getSender())
			.append(", <=FreeStableIndex ").append(index)
			.append(", swapOutLearner ").append(swapOutLearner)
			.append(">");
		return sb.toString();
	}
}
