package glassPaxos.network.messages;

import glassPaxos.network.NodeIdentifier;
import io.netty.buffer.ByteBuf;

import java.util.*;

public class LearnerCatchupMessage extends Message {

	//private long first_unstable_index;
	private long from;
	/* numOfDecisions = (to - from + 1); */
	private int numOfDecisions;
	byte[] decisionsBytes;

	protected LearnerCatchupMessage(){}

	public LearnerCatchupMessage(NodeIdentifier sender, long from, int num, byte[] decisions) {
		super(MSG_TYPE.LCATCHUP, sender);
		this.from = from;
		this.numOfDecisions = num;
		this.decisionsBytes = decisions;
	}

	public long getFrom() {
		return from;
	}

	public int getNumOfDecisions() {
		return numOfDecisions;
	}

	public byte[] getDecisionsBytes() {
		return decisionsBytes;
	}

	@Override
	public void serialize(ByteBuf buf){
		super.serialize(buf);
		buf.writeLong(from);
		buf.writeInt(numOfDecisions);
		if (decisionsBytes != null) {
			buf.writeInt(decisionsBytes.length);
			buf.writeBytes(decisionsBytes);
		} else {
			buf.writeInt(0);
		}
	}
	
	@Override
	public void deserialize(ByteBuf buf){
		super.deserialize(buf);
        //first_unstable_index = buf.readLong();
		from = buf.readLong();
		numOfDecisions = buf.readInt();

		int len = buf.readInt();
		if (len > 0) {
			decisionsBytes = new byte[len];
			buf.readBytes(decisionsBytes, 0, len);
		} else {
			decisionsBytes = null;
		}

	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("LearnerCatchupMessage<src ").append(super.getSender());
		//sb.append(", first_unstable: ").append(first_unstable_index);
		sb.append(", from ").append(from);
		sb.append(", #decisions ").append(numOfDecisions);
		if (decisionsBytes != null) {
			sb.append(", decisionLen ").append(decisionsBytes.length).append(">");
		} else {
			sb.append(", null>");
		}
		return sb.toString();
	}
}
