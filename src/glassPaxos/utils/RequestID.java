package glassPaxos.utils;

import glassPaxos.network.NodeIdentifier;

@Deprecated
public class RequestID {

    //todo: if need sequential execution, then need implements Comparable
    private int hashCode;
    private long request_num;

    public RequestID(NodeIdentifier cid, long request_num) {
        this.request_num = request_num;
        int hashNum = (int)(request_num ^ (request_num>>>32));
        hashCode = (cid.getID() << 24) | (hashNum & 0x0FFF);
    }

//    public RequestID(int hashCode){
//        this.hashCode = hashCode;
//    }

    public NodeIdentifier getReqid() {
        return new NodeIdentifier(NodeIdentifier.Role.CLIENT, hashCode>>24);
    }

    public long getReqnum() {
//        return hashCode & 0x0FFF;
        return request_num;
    }

    public int hashCode() {
        return hashCode;
    }

	public String toString(){
		return String.format("request_id<cid %s, req_num: %d>",
                getReqid().toString(), request_num);
	}
}

