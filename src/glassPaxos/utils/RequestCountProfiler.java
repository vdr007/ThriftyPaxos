package glassPaxos.utils;

public class RequestCountProfiler {

    private long received;
    private long rejected;
    private long executed;
    private long processed;
    private long dropped;
    private long accepted;

    private long lastReceived;
    private long lastRejected;
    private long lastExecuted;
    private long lastProcessed;
    private long lastDropped;
    private long lastAccepted;

    public RequestCountProfiler() {
        received = 0;
        rejected = 0;
        executed = 0;
        processed = 0;
        dropped = 0;
        accepted = 0;
        lastReceived = 0;
        lastRejected = 0;
        lastExecuted = 0;
        lastProcessed = 0;
        lastDropped = 0;
        lastAccepted = 0;
    }

    public synchronized void addReceived() {
        this.received += 1;
    }

    public void addRejected() {
        this.rejected += 1;
    }

    public synchronized void addExecuted() {
        this.executed += 1;
    }

    public synchronized void addProcessed() {
        this.processed += 1;
    }

    public synchronized void addDropped() {
        this.dropped += 1;
    }

    public synchronized void addAccepted() {
        this.accepted += 1;
    }

    public synchronized String displayCounterChange() {
        StringBuilder sb = new StringBuilder();
        sb.append("reqCountInfo: rejected ").append(rejected-lastRejected)
                .append(", received ").append(received-lastReceived)
                .append(", *executed ").append(executed-lastExecuted)
                .append(", processed ").append(processed-lastProcessed)
                .append(", dropped ").append(dropped-lastDropped)
                //.append(", diff ").append(received-(executed+processed+dropped))
                .append(", accepted ").append(accepted-lastAccepted)
                .append("\n");
        return sb.toString();
    }

    public synchronized void updateLastCounters() {
        this.lastReceived = this.received;
        this.lastRejected = this.rejected;
        this.lastExecuted = this.executed;
        this.lastProcessed = this.processed;
        this.lastDropped = this.dropped;
        this.lastAccepted = this.accepted;
    }
}
