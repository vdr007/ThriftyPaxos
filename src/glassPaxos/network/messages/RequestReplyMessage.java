package glassPaxos.network.messages;

import glassPaxos.network.NodeIdentifier;
import io.netty.buffer.ByteBuf;

public class RequestReplyMessage extends Message {
	
	private long reqNum;
	//private int rLen;
	//private String result;
	private byte []reply;

	protected RequestReplyMessage(){}
	
	public RequestReplyMessage(NodeIdentifier sender, long reqNum, byte []reply) {
		super(Message.MSG_TYPE.REQ_REPLY, sender);
		this.reqNum = reqNum;
		this.reply = reply;
	}

	public long getReqNum(){
		return reqNum;
	}

	public byte[] getReply() {
		return reply;
	}

	//public int getrLen() {
	//	return rLen;
	//}
	//
	//public String getResult(){
    //    return result;
	//}
	
	@Override
	public void serialize(ByteBuf buf){
		super.serialize(buf);
		buf.writeLong(reqNum);
        if (reply != null) {
		    buf.writeInt(reply.length);
		    buf.writeBytes(reply);
        } else {
            buf.writeInt(0);
        }
	}
	
	@Override
	public void deserialize(ByteBuf buf){
		super.deserialize(buf);
		reqNum = buf.readLong();
		int len = buf.readInt();
        if (len > 0) {
		    reply = new byte[len];
		    buf.readBytes(reply, 0, len);
        } else {
            reply = null;
        }
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("RequestReplyMessage <src ").append(super.getSender())
				.append(", reqNum ").append(reqNum).append(", reply ");
        if (reply != null) {
            sb.append(reply);
        } else {
            sb.append("NOREPLY");
        }
        sb.append(">");
		return sb.toString();
	}

}
