package glassPaxos.network.messages;
import glassPaxos.network.NodeIdentifier;
import io.netty.buffer.ByteBuf;

public class PromiseMessage extends Message {
	
	//private int bnHash; 			//hashcode of ballot number
	private int round; 				//round-robin leader election algorithm
	private long maxAcceptedIndex;  //conservatively start from maxAcceptedIndex+1
    private long maxLogIndex;       //aggressively start from maxLogIndex+1

	protected PromiseMessage(){}
	
	public PromiseMessage(NodeIdentifier sender, int round, long maxAccepted, long maxLog) {
		super(Message.MSG_TYPE.PROMISE, sender);
		this.round = round;
		this.maxAcceptedIndex = maxAccepted;
		this.maxLogIndex = maxLog;
	}

	// public int getBnHash() {
	// 	return bnHash;
	// }

	public int getRound() {
		return round;
	}

	public long getMaxAcceptedIndex() {
		return maxAcceptedIndex;
	}

	public long getMaxLogIndex() {
		return maxLogIndex;
	}
	
	@Override
	public void serialize(ByteBuf buf){
		super.serialize(buf);
		buf.writeInt(round);
		buf.writeLong(maxAcceptedIndex);
		buf.writeLong(maxLogIndex);
	}
	
	@Override
	public void deserialize(ByteBuf buf){
		super.deserialize(buf);
        round = buf.readInt();
        maxAcceptedIndex = buf.readLong();
        maxLogIndex = buf.readLong();
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("PromiseMessage<src ").append(super.getSender())
			.append(", round ").append(round)
			.append(", maxAccepted ").append(maxAcceptedIndex)
			.append(", maxLog ").append(maxLogIndex).append(">");
		return sb.toString();
	}

}
