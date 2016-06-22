package glassPaxos.acceptor;

import java.util.HashMap;

public class Quorum {
    private int total;
    private int quorum; //count value
    // once in the case of leader failure, new leader threw ConcurrentModificationException in swapEntry
    public HashMap<Integer, QuorumEntry> qrmap = new HashMap<Integer, QuorumEntry>();

    public Quorum(int total, int quorum) {
        this.total = total;
        this.quorum = quorum;
    }

    public synchronized int getQuorum() {
        return quorum;
    }

    public synchronized boolean recvAll() {
        return total <= quorum;
    }

    public synchronized boolean recvQuorum() {
        return (quorum*2 > total);
    }

    public synchronized int getTotal() {
        return total;
    }

    public synchronized void incQuorum() {
        this.quorum = this.quorum+1;
    }

    public synchronized void resetQuorum() {
        this.quorum = 0;
    }
}

