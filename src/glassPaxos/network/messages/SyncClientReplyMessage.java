package glassPaxos.network.messages;
import io.netty.buffer.ByteBuf;
import glassPaxos.network.NodeIdentifier;

public class SyncClientReplyMessage extends Message {
    private long lastRequestNum;

    protected SyncClientReplyMessage(){}

    public SyncClientReplyMessage(NodeIdentifier sender, long lastRequestNum) {
        super(Message.MSG_TYPE.SYNC_CLIENT_REPLY, sender);
        this.lastRequestNum = lastRequestNum;
    }

    public long getLastRequestNum() {
        return lastRequestNum;
    }

    @Override
    public void serialize(ByteBuf buf){
        super.serialize(buf);
        buf.writeLong(lastRequestNum);
    }

    @Override
    public void deserialize(ByteBuf buf){
        super.deserialize(buf);
        lastRequestNum = buf.readLong();
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("SyncClientReplyMessage <src ").append(super.getSender())
                .append(", lastRequestNum ").append(lastRequestNum).append(">");
        return sb.toString();
    }

}
