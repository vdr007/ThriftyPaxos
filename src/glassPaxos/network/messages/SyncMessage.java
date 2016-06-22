package glassPaxos.network.messages;
import glassPaxos.network.NodeIdentifier;
import io.netty.buffer.ByteBuf;

public class SyncMessage extends Message {
	
	private long start;
	private int len;

	protected SyncMessage(){}
	
	public SyncMessage(NodeIdentifier sender, long start, int len) {
		super(Message.MSG_TYPE.SYNC, sender);
		this.start = start;
		this.len = len;
	}

	public long getStart() {
		return start;
	}

	public int getLen() {
		return len;
	}

	@Override
	public void serialize(ByteBuf buf){
		super.serialize(buf);
		buf.writeLong(start);
		buf.writeInt(len);
	}
	
	@Override
	public void deserialize(ByteBuf buf){
		super.deserialize(buf);
        start = buf.readLong();
		len = buf.readInt();
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("SyncMessage <src ").append(super.getSender())
			.append(", start ").append(start).append(", len ").append(len).append(">");
		return sb.toString();
	}

}
