package glassPaxos.utils;

import glassPaxos.network.NodeIdentifier;

public interface RequestCallback {
    public void processResult(NodeIdentifier server, int req_num, int rc);
}
