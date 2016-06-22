package glassPaxos.network.messages;

import glassPaxos.network.NodeIdentifier;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

public class DecisionMessage extends Message {
	
	// private int reqID;
	// private long reqNum;
	private long index;
	private byte[] request;

	public DecisionMessage(){}

	/* copy constructor */
	public DecisionMessage(DecisionMessage m) {
		super(MSG_TYPE.DECISION, m.getSender());
		// reqID = m.getReqID();
		// reqNum = m.getReqNum();
		index = m.getIndex();
		request = m.getRequest();
	}

	/* normal constructor */
	public DecisionMessage(NodeIdentifier sender, long index, byte[] request) {
		super(Message.MSG_TYPE.DECISION, sender);
		// this.reqID = reqID;
		// this.reqNum = reqNum;
		this.index = index;
		this.request = request;
	}
	
	// public long getReqNum(){
	// 	return reqNum;
	// }

	public long getIndex(){
		return index;
	}

	// public int getReqID(){
	// 	return reqID;
	// }

	public byte[] getRequest() {
		return request;
	}

	public byte[] toByteArray(){
		byte[] ret = new byte[4+4 + 4+8+8 + 4+request.length];
		ByteBuffer b = ByteBuffer.wrap(ret);
		b.putInt(getType());
		b.putInt(getSender().hashCode());

		// b.putInt(reqID);
		// b.putLong(reqNum);
		b.putLong(index);

		b.putInt(request.length);
		b.put(request);
		return ret;
	}

	public void fromByteArray(byte[] bytes){
		ByteBuffer b = ByteBuffer.wrap(bytes);
		super.setType(b.getInt());
		super.setSender(b.getInt());

		// reqID = b.getInt();
		// reqNum = b.getLong();
		index = b.getLong();

		request = new byte[b.getInt()];
		b.get(request);
	}

	@Override
	public void serialize(ByteBuf buf){
		super.serialize(buf);
		// buf.writeInt(reqID);
		// buf.writeLong(reqNum);
		buf.writeLong(index);
        if (request != null) {
		    buf.writeInt(request.length);
		    buf.writeBytes(request);
        } else {
		    buf.writeInt(0);
        }
	}
	
	@Override
	public void deserialize(ByteBuf buf){
		super.deserialize(buf);
        // reqID = buf.readInt();
		// reqNum = buf.readLong();
        index = buf.readLong();
      	int len = buf.readInt();
        if (len > 0) {
      		request = new byte[len];
			buf.readBytes(request, 0, len);
        } else {
            request = null;
        }
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("DecisionMessage <src ").append(super.getSender())
			// .append(", reqID ").append(reqID)
			// .append(", reqNum ").append(reqNum)
			.append(", LogIndex ").append(index).append(", ");

		if (/*reqNum > 0 &&*/ request != null) {
			//sb.append(RequestMessage.bytesToString(request));
			sb.append(", reqLen=").append(request.length);
		} else {
			sb.append("NO-OP");
		}
		sb.append(">");
		return sb.toString();
	}

}
