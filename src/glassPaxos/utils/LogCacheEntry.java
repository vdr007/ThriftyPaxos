package glassPaxos.utils;

import glassPaxos.client.TestEntry;

@Deprecated
public class LogCacheEntry {
//    private int reqID;
//    private long reqNum;
//    private int hashReqNum;
    private int bn;
    private boolean done;
    private byte[] testEntry;

    public LogCacheEntry(int bn, boolean done, byte[] testEntry) {
//        this.reqID = reqID;
//        this.reqNum = reqNum;
//        this.hashReqNum = hashReqNum;
        this.bn = bn;
        this.done = done;
        this.testEntry = testEntry;
    }

//    public int getReqID() {
//        return reqID;
//    }

//    public long getReqNum() {
//        return reqNum;
//    }

//    public int getHashReqNum() {
//        return hashReqNum;
//    }

//    public int getClientID() {
//        return (hashReqNum>>24);
//    }

//    public int getModReqNum(){
//        return hashReqNum & 0x0FFF;
//    }

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

    public byte[] getTestEntry() {
        return testEntry;
    }

    public String toString() {
        return String.format("LogCacheEntry <bn %d, done %b, entry %s",
                bn, done, TestEntry.transformString(testEntry));
    }

}

