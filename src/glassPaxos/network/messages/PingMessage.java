package glassPaxos.network.messages;
import glassPaxos.network.NodeIdentifier;
import io.netty.buffer.ByteBuf;

public class PingMessage extends Message {

	private long info;

	protected PingMessage(){}
	
	public PingMessage(NodeIdentifier sender, long info) {
		super(Message.MSG_TYPE.PING, sender);
		this.info = info;
	}

	public long getInfo(){
		return info;
	}
	
	@Override
	public void serialize(ByteBuf buf){
		super.serialize(buf);
		buf.writeLong(info);
	}
	
	@Override
	public void deserialize(ByteBuf buf){
		super.deserialize(buf);
		info = buf.readLong();
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("PingMessage <src ").append(super.getSender())
			.append(", info ").append(info).append(">");
		return sb.toString();
	}
}
