package glassPaxos.network.messages;

import glassPaxos.network.NodeIdentifier;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class AcceptorCatchupMessage extends Message {

	//private boolean inCache;
	private int mode; //mode 0) general, 1) rePropose
	//private long first_unstable_index;
	private long from;
	/* numOfAccepts = (to - from + 1); */
	private int numOfAccepts;
	byte []acceptsBytes;

	protected AcceptorCatchupMessage(){}

	public AcceptorCatchupMessage(NodeIdentifier sender, int mode, long from, int num, byte[] accBytes) {
		super(MSG_TYPE.ACATCHUP, sender);
		//this.inCache = inCache;
		this.mode = mode;
		//this.first_unstable_index = first_unstable_index;
		this.from = from;
		this.numOfAccepts = num;
		this.acceptsBytes = accBytes;
	}

	//public boolean getInCache() {
	//	return inCache;
	//}

	//public long getFirst_unstable_index() {
	//	return first_unstable_index;
	//}

	public long getFrom() {
		return from;
	}

	public int getNumOfAccepts() {
		return numOfAccepts;
	}

	public byte[] getAcceptsBytes() {
		return acceptsBytes;
	}

	@Override
	public void serialize(ByteBuf buf){
		super.serialize(buf);
		//buf.writeBoolean(inCache);
		//buf.writeLong(first_unstable_index);
		buf.writeInt(mode);
		buf.writeLong(from);

		buf.writeInt(numOfAccepts);
        if (acceptsBytes != null) {
		    buf.writeInt(acceptsBytes.length);
		    buf.writeBytes(acceptsBytes);
        } else {
			buf.writeInt(0);
		}
	}
	
	@Override
	public void deserialize(ByteBuf buf){
		super.deserialize(buf);
		//inCache = buf.readBoolean();
        //first_unstable_index = buf.readLong();
		mode = buf.readInt();
		from = buf.readLong();
		numOfAccepts = buf.readInt();
		int len = buf.readInt();

        if (len > 0) {
		    acceptsBytes = new byte[len];
		    buf.readBytes(acceptsBytes, 0, len);
        } else {
            acceptsBytes = null;
        }
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("AcceptorCatchupMessage< src ").append(super.getSender())
			//.append(", firstUn ").append(first_unstable_index)
			.append(", mode ").append(mode)
			.append(", from ").append(from)
			.append(", #dec ").append(numOfAccepts);

		if (acceptsBytes != null)
			sb.append(", decisionBytesLen ").append(acceptsBytes.length);

		sb.append(">");

		return sb.toString();
	}

	public static void main(String []args) throws Exception {
		//unit test of AcceptorCatchupMessage
		NodeIdentifier src = new NodeIdentifier(NodeIdentifier.Role.CLIENT, 99);
		int num = 5;
		int bbLen = 0;
		List<byte[]> tmp = new ArrayList<>();

		ClientRequest cr;
		for (int i=0;i<num;i++) {
			String s = "hello " + String.valueOf(i) + "world";
			cr = new ClientRequest(i, i, s.getBytes());
			AcceptMessage acc = new AcceptMessage(src, i+10, 1L, 1L, cr.toRequestByteArray());
			tmp.add(acc.toByteArray());
			bbLen += acc.toByteArray().length;
		}
		byte[] chunk = new byte[bbLen + 4 * num + 4];
		ByteBuffer bb = ByteBuffer.wrap(chunk);
		bb.putInt(num); //#AcceptorMessages
		for (byte []b : tmp) {
			bb.putInt(b.length);
			bb.put(b);
		}
		AcceptorCatchupMessage msg = new AcceptorCatchupMessage(src, 1, 5L, num, chunk);
		System.out.format("UNIT TEST fabricate %s send ==> \n%s \n", src, msg.toString());

		bb = ByteBuffer.wrap(chunk);
		int sz = bb.getInt();
		byte []str;
		for (int i=0; i<sz; i++) {
			str = new byte[bb.getInt()];
			bb.get(str);
			AcceptMessage m = new AcceptMessage();
			m.readFromByteArray(str);
			System.out.format("==> log update: [%d] AcceptMsg %s\n",
					i, m.toString());
		}

	}

	public int getMode() {
		return mode;
	}
}
