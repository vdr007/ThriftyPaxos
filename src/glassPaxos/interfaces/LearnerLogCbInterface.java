package glassPaxos.interfaces;

public interface LearnerLogCbInterface {
    void batchProcessLearnerLog(long start);
    void syncLogComplete();
}
