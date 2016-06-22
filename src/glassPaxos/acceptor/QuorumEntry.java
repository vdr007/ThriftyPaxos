package glassPaxos.acceptor;

import glassPaxos.network.NodeIdentifier;

/* settings:
 * init: (false, 0)
 * set/update: (false, timestamp)
 * done: (true, 0)
 */
public class QuorumEntry {
    //private NodeIdentifier qid;
    private boolean done;
    private long timestamp;

    public QuorumEntry(boolean done, long timestamp) {
        //this.qid = qid;
        this.done = done;
        this.timestamp = timestamp;
    }

    public boolean getDone() {
        return done;
    }

    public long getTimeStamp() {
        return timestamp;
    }

    public void setTimeStamp(long time) {
        timestamp = time;
    }

    @Override
    public String toString() {
        return String.format("QuorumEntry[done %b, ts %d]", done, timestamp);
    }
}
