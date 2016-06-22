package glassPaxos.network.messages;

import glassPaxos.network.NodeIdentifier;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;

public class InfoMessage extends Message {

	private byte[] info;
	protected InfoMessage(){}

	public InfoMessage(NodeIdentifier sender, String str) {
		super(MSG_TYPE.INFO, sender);
		this.info = str.getBytes();
	}
	
	public String getInfo(){
		return new String(info, StandardCharsets.UTF_8);
	}
	
	@Override
	public void serialize(ByteBuf buf){
		super.serialize(buf);
		if (info!=null) {
			buf.writeInt(info.length);
			buf.writeBytes(info);
		} else {
			buf.writeInt(0);
		}
	}
	
	@Override
	public void deserialize(ByteBuf buf){
		super.deserialize(buf);
		int len = buf.readInt();
		if (len > 0) {
			info = new byte[len];
			buf.readBytes(info, 0, len);
		} else {
			info = null;
		}
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("InfoMessage<src ").append(super.getSender());
		if (info!=null) {
			sb.append(", ").append(getInfo()).append(">");
		} else {
			sb.append(", null>");
		}
		return sb.toString();
	}
}
