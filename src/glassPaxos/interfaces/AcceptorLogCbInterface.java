package glassPaxos.interfaces;

public interface AcceptorLogCbInterface {
    void backgroundTask(long index);
}
