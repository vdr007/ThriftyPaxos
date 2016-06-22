package glassPaxos.network.messages;

import glassPaxos.network.NodeIdentifier;
import glassPaxos.network.NodeIdentifier.Role;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

public class Message {

	//protected static final int MSG_TEST = 0;
    public enum MSG_TYPE {
	    TEST, INFO, PING,
	    REQ, REQ_REPLY,
	    PREPARE, PROMISE,
	    ACCEPT, ACCEPTED, BACCEPTED, DECISION,
	    SYNC, SYNC_CLIENT_REPLY, SYNCSNAPSHOT,
		FREELOG, ACATCHUP,
		SNAPSHOTCHUNK, LCATCHUP,
		SLEEPDOWN, FAKELEARNERSYNC,
		WAKEUP
    }
	
	private int type;
	private NodeIdentifier sender;
	
	protected Message(){}
	
	public Message(MSG_TYPE msgtype, NodeIdentifier sender){
		this.type = msgtype.ordinal();
		this.sender = sender;
	}

    public int getType() {
        return type;
    }

	public void setType(int t) {
		type = t;
	}

	public NodeIdentifier getSender(){
		return sender;
	}

	public void setSender(int hashCode){
		sender = new NodeIdentifier(hashCode);
	}

	public NodeIdentifier.Role getSenderRole(){
		return sender.getRole();
	}
	
	public int getSenderID(){
		return sender.getID();
	}
	
	public void serialize(ByteBuf buf){
		buf.writeInt(type);
		buf.writeInt(sender.hashCode());
	}
	
	public void deserialize(ByteBuf buf){
		type = buf.readInt();
		sender = new NodeIdentifier(buf.readInt());
	}
	
	public static Message deserializeRaw(ByteBuf buf){
		Message ret = null;
		buf.markReaderIndex();
		int type = buf.readInt();
		buf.resetReaderIndex();
        //System.out.println("call deserializeRaw with type: " + MSG_TYPE.values()[type] + "\n");
		switch(MSG_TYPE.values()[type]){
			case TEST:
				ret = new TestMessage();
				break;
			case INFO:
				ret = new InfoMessage();
				break;
			case PING:
				ret = new PingMessage();
				break;
			case PREPARE:
				ret = new PrepareMessage();
				break;
			case PROMISE:
				ret = new PromiseMessage();
				break;
			case ACCEPT:
				ret = new AcceptMessage();
				break;
			case ACCEPTED:
				ret = new AcceptedMessage();
				break;
			case BACCEPTED:
				ret = new BatchAcceptedMessage();
				break;
			case REQ:
				ret = new RequestMessage();
				break;
			case REQ_REPLY:
				ret = new RequestReplyMessage();
				break;
			case DECISION:
				ret = new DecisionMessage();
				break;
			case FREELOG:
				ret = new FreeLogMessage();
				break;
			case SYNC:
				ret = new SyncMessage();
				break;
			case SLEEPDOWN:
				ret = new SleepDownMessage();
				break;
			case WAKEUP:
				ret = new WakeupMessage();
				break;
			case ACATCHUP:
				ret = new AcceptorCatchupMessage();
				break;
			case LCATCHUP:
				ret = new LearnerCatchupMessage();
				break;
			case SNAPSHOTCHUNK:
				ret = new SnapshotChunkMessage();
				break;
			case FAKELEARNERSYNC:
				ret = new FakeLearnerSyncReplyMessage();
				break;
			case SYNC_CLIENT_REPLY:
				ret = new SyncClientReplyMessage();
				break;
			case SYNCSNAPSHOT:
				ret = new SyncSnapshotMessage();
				break;
			default:
				throw new RuntimeException("Unknown msg type "+type);
		}
		ret.deserialize(buf);
		return ret;
	}
}
