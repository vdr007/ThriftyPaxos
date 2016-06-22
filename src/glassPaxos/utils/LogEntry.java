package glassPaxos.utils;

@Deprecated
public class LogEntry {
    private int reqID;
    private long reqNum;
    private int bn;
    private boolean done;
    private byte[] request;

    public LogEntry(int reqID, long reqNum, int bn, boolean done, byte[] request) {
        this.reqID = reqID;
        this.reqNum = reqNum;
        this.bn = bn;
        this.done = done;
        this.request = request;
    }

    public int getReqID() {
        return reqID;
    }

    public long getReqNum() {
        return reqNum;
    }

    public int getBn() {
        return bn;
    }

    public void setBn(int bn_hc) {
        bn = bn_hc;
    }

//    public TestEntry.OP_TYPE getOpvalue() {
//        return TestEntry.OP_TYPE.values()[op];
//    }

//    public int getOp() {
//        return op;
//    }

//    public String getValue() {
//        return value;
//    }

//    public void setValue(String value) {
//        this.value = value;
//    }

    public boolean getDone() {
        return done;
    }

    public void setDone(boolean done) {
        this.done = done;
    }

//    public int getvLen() {
//       return vLen;
//    }

    public byte[] getRequest() {
        return request;
    }

    public String toString() {
        return String.format("log <req[%d, %d], bn %d, done %b, request %s",
                reqID, reqNum, bn, done, request.toString());
    }
}

