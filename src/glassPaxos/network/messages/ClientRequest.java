package glassPaxos.network.messages;

import java.nio.ByteBuffer;

public class ClientRequest {
    private int clientID;
    private long reqNum;
    private byte[] request;

    public ClientRequest() {}

    public ClientRequest(int clientID, long reqNum, byte[] request) {
        this.clientID = clientID;
        this.reqNum = reqNum;
        this.request = request;
    }

    public int getClientID() {
        return clientID;
    }

    public long getReqnum() {
        return reqNum;
    }

    public byte[] getRequest() {
        return request;
    }

    public byte[] toRequestByteArray(){
        byte[] ret;
        if (request != null) {
            ret = new byte[4 + 8 + 4 + request.length]; //cid(int), reqNum(long), req.length
            ByteBuffer b = ByteBuffer.wrap(ret);
            b.putInt(clientID);
            b.putLong(reqNum);

            b.putInt(request.length);
            b.put(request);
        } else {
            ret = new byte[4 + 8 + 4]; //cid(int), reqNum(long), req.length
            ByteBuffer b = ByteBuffer.wrap(ret);
            b.putInt(clientID);
            b.putLong(reqNum);
            b.putInt(0);
        }
        return ret;
    }

    public void readFromRequestByteArray(byte[] bytes){
        ByteBuffer b = ByteBuffer.wrap(bytes);
        clientID = b.getInt();
        reqNum = b.getLong();

        int len = b.getInt();
        //System.out.printf("cid=%d, reqNum=%d, len=%d\n", clientID, reqNum, len);
        if (len > 0) {
            request = new byte[len];
            b.get(request);
        } else {
            request = null;
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ClientRequest<id=").append(clientID)
                .append(", reqNum=").append(reqNum);

        if (reqNum > 0 && request != null) {
            //sb.append(RequestMessage.bytesToString(request));
            sb.append(", reqLen=").append(request.length);
        } else {
            sb.append(", NO-OP");
        }
        sb.append(">");

        return sb.toString();
    }
}

