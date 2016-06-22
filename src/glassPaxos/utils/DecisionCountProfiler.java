package glassPaxos.utils;

public class DecisionCountProfiler {

    private long resubmit;
    private long drop;
    private long accept;
    private long noOp;
    private long exec;

    private long lastResubmit;
    private long lastDrop;
    private long lastAccept;
    private long lastNoOp;
    private long lastExec;

    public DecisionCountProfiler() {
        resubmit = 0;
        drop = 0;
        accept = 0;
        noOp = 0;
        exec = 0;
        lastResubmit = 0;
        lastDrop = 0;
        lastAccept = 0;
        lastNoOp = 0;
        lastExec = 0;
    }

    public synchronized void addResubmit() {
        this.resubmit += 1;
    }

    public void addDrop() {
        this.drop += 1;
    }

    public synchronized void addAccept() {
        this.accept += 1;
    }

    public synchronized void addNoop() {
        this.noOp += 1;
    }

    public synchronized void addExec() {
        this.exec += 1;
    }

    public synchronized String displayCounterChange() {
        StringBuilder sb = new StringBuilder();
        sb.append("DecisionCountInfo: resubmit ").append(resubmit-lastResubmit)
                .append(", drop ").append(drop-lastDrop)
                .append(", accept ").append(accept-lastAccept)
                .append(", noOp ").append(noOp-lastNoOp)
                .append(", *exec ").append(exec-lastExec)
                .append("\n");
        return sb.toString();
    }

    public synchronized void updateLastCounters() {
        this.lastResubmit = this.resubmit;
        this.lastDrop = this.drop;
        this.lastAccept = this.accept;
        this.lastNoOp = this.noOp;
        this.lastExec = this.exec;
    }
}
