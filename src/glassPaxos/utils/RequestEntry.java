package glassPaxos.utils;

import glassPaxos.interfaces.AppClientCallback;

public class RequestEntry {
    private long reqNum;
    private byte[] req;
    private boolean resubmit; //true if resubmitted
    private long cTime; //in ms
    private long initTime; //in ns
    private AppClientCallback cb;
    private byte[] reply;

    public RequestEntry(long reqNum, byte[] req, boolean resubmit, long cTime, long initTime,
                        AppClientCallback cb, byte[] reply) {
        this.reqNum = reqNum;
        this.req = req;
        this.resubmit = resubmit;
        this.cTime = cTime;
        this.initTime = initTime;
        this.cb = cb;
        this.reply = reply;
    }

    public long getReqnum() {
        return reqNum;
    }

    public byte[] getReq() {
        return req;
    }

    public boolean isResubmit() {
        return resubmit;
    }

    public void setResubmit(boolean val) {
        resubmit = val;
    }

    public long getInitTime() {
        return initTime;
    }

    public long getcTime() {
        return cTime;
    }

    public void setcTime(long current) {
        cTime = current;
    }

    public AppClientCallback getCb() {
        return cb;
    }

    public void setReply(byte[] reply) {
        this.reply = reply;
    }

    public byte[] getReply() {
        return reply;
    }

    public String toString() {
        return String.format(
                "request[%d]: reSub %b, cTime %d, cb %s, req: %s",
                reqNum, resubmit, cTime, cb, req.toString());
    }
}

