package glassPaxos.network.messages;

import glassPaxos.network.NodeIdentifier;
import io.netty.buffer.ByteBuf;

public class TestMessage extends Message {
	
	private int value;

	protected TestMessage(){}
	
	public TestMessage(NodeIdentifier sender, int value) {
		super(Message.MSG_TYPE.TEST, sender);
		this.value = value;
	}
	
	public int getValue(){
		return value;
	}
	
	@Override
	public void serialize(ByteBuf buf){
		super.serialize(buf);
		//for(int i=0; i<10000; i++)
			buf.writeInt(value);
	}
	
	@Override
	public void deserialize(ByteBuf buf){
		super.deserialize(buf);
		//for(int i=0; i<10000; i++)
			value = buf.readInt();
	}

	@Override
	public String toString(){
		return "TestMessage src=" + super.getSender() + ", value="+value;
	}
}
