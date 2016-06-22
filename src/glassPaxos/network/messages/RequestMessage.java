package glassPaxos.network.messages;

import glassPaxos.network.NodeIdentifier;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;

public class RequestMessage extends Message {

	private long reqNum;
	private byte[] request;

	protected RequestMessage(){}

	public RequestMessage(NodeIdentifier sender, long reqNum, byte[] request) {
		super(Message.MSG_TYPE.REQ, sender);
		this.reqNum = reqNum;
		this.request = request;
	}

	public long getReqnum(){
        return reqNum;
	}

	public byte[] getRequest() {
		return request;
	}

	public static String bytesToString(byte []request) {
		StringBuilder sBuf = new StringBuilder();
		if (request != null)
			sBuf.append("request[] len: " ).append(request.length);
		else
			sBuf.append("request[] null" );
		return sBuf.toString();
	}

	public static String OldbytesToString(byte []input) {
		StringBuilder strBuf = new StringBuilder();
		ByteBuffer b = ByteBuffer.wrap(input);
		byte mode = b.get();
		long key = b.getLong();
		strBuf.append("(key: " ).append(key).append(", op: ");
		if(mode == (byte)1) {
			strBuf.append("put");
		} else {
			strBuf.append("get");
		}
		byte[] val = new byte[b.remaining()];
		b.get(val);
		//String objects will bring GC burden, enable it only in debug test
		//String sVal = null;
		//try {
		//	sVal = new String(val, "UTF-8");
		//} catch (UnsupportedEncodingException e) {
		//	e.printStackTrace();
		//}
		//strBuf.append(", value: " ).append(sVal).append(")");
		return strBuf.toString();
	}

	@Override
	public void serialize(ByteBuf buf){
		super.serialize(buf);
		buf.writeLong(reqNum);
		int len = request.length;
		buf.writeInt(len);
		buf.writeBytes(request);
	}
	
	@Override
	public void deserialize(ByteBuf buf){
		super.deserialize(buf);
        reqNum = buf.readLong();
		int len = buf.readInt();
		request = new byte[len];
		buf.readBytes(request, 0, len);
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("RequestMessage <src ").append(super.getSender())
		.append(", reqNum ").append(reqNum).append(", ")
		.append(bytesToString(request)).append(">");
		return sb.toString();
	}

}
