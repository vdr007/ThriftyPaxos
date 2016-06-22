package glassPaxos.network.messages;

import glassPaxos.network.NodeIdentifier;
import glassPaxos.utils.BallotNumber;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

public class AcceptMessage extends Message {

	//private int clientIDHash;
	//private long clientReqNum;
	private int round;
	private long index;
	private long firstUnstable;
	private byte[] request;

	public AcceptMessage(){}
	
	public AcceptMessage(NodeIdentifier sender, int round, long index, long first, byte[] request) {
		super(Message.MSG_TYPE.ACCEPT, sender);
		//this.clientIDHash = cID;
		//this.clientReqNum = reqNum;
		this.round = round;
		this.index = index;
		this.firstUnstable = first;
		this.request = request;
	}

	public AcceptMessage(AcceptMessage m) {
		super(MSG_TYPE.ACCEPT, m.getSender());
		//clientIDHash = m.getClientIDHash();
		//clientReqNum = m.getClientReqNum();
		round = m.getRound();
		index = m.getIndex();
		firstUnstable = m.getFirstUnstable();
		request = m.getRequest();
	}

	// public int getBn() {
	// 	BallotNumber bn = new BallotNumber(getSender(), round);
	// 	return bn.hashCode();
	// }

	// public int getClientIDHash() {
	// 	return clientIDHash;
	// }

	// public long getClientReqNum() {
	// 	return clientReqNum;
	// }

	public int getRound(){
		return round;
	}

	public void setRound(int newRound) {
		round = newRound;
	}
	
	public long getIndex(){
		return index;
	}
	
	public long getFirstUnstable(){
		return firstUnstable;
	}

	public void setFirstUnstable(long firstUnstable) {
		this.firstUnstable = firstUnstable;
	}

	public byte[] getRequest() {
		return request;
	}

	public byte[] toByteArray(){
		byte[] ret = new byte[4+4 + 4+8+4+8+8+ 4+request.length];
		ByteBuffer b = ByteBuffer.wrap(ret);
		b.putInt(getType());
		b.putInt(getSender().hashCode());

		// b.putInt(clientIDHash);
		// b.putLong(clientReqNum);
		b.putInt(round);
		b.putLong(index);
		b.putLong(firstUnstable);

		b.putInt(request.length);
		b.put(request);
		return ret;
	}

	public void readFromByteArray(byte[] bytes){
		ByteBuffer b = ByteBuffer.wrap(bytes);
		super.setType(b.getInt());
		super.setSender(b.getInt());

		// clientIDHash = b.getInt();
		// clientReqNum = b.getLong();
		round = b.getInt();
		index = b.getLong();
		firstUnstable = b.getLong();

		request = new byte[b.getInt()];
		b.get(request);
	}

	@Override
	public void serialize(ByteBuf buf){
		super.serialize(buf);
		// buf.writeInt(clientIDHash);
		// buf.writeLong(clientReqNum);
		buf.writeInt(round);
		buf.writeLong(index);
		buf.writeLong(firstUnstable);
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
		// clientIDHash = buf.readInt();
		// clientReqNum = buf.readLong();
        round = buf.readInt();
        index = buf.readLong();
        firstUnstable = buf.readLong();
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
		sb.append("AcceptMessage<src ").append(super.getSenderID())
			// .append(", req# ").append(clientIDHash)
			// .append(":").append(clientReqNum)
			.append(", round ").append(round)
			.append(", LogIndex ").append(index)
			.append(", Unstable ").append(firstUnstable);

		if (/* clientReqNum > 0 && */ request != null) {
			//sb.append(RequestMessage.bytesToString(request));
			sb.append(", reqLen ").append(request.length);
		} else {
			sb.append(", NO-OP");
		}
		sb.append(">");

		return sb.toString();
	}

	public static void main(String []args) throws Exception{
		NodeIdentifier src = new NodeIdentifier(NodeIdentifier.Role.CLIENT, 77);
		int i = 10;
		String s = "hello " + String.valueOf(i) + "world";
		ClientRequest cr = new ClientRequest(i, i, s.getBytes());
		AcceptMessage acc = new AcceptMessage(src, 10, 100, 101, cr.toRequestByteArray());
		byte[] req = acc.getRequest();
		ClientRequest recvCr = new ClientRequest();
		recvCr.readFromRequestByteArray(req);
		System.out.printf("send request=%s, recv=%s\n", cr, recvCr);
	}
}
