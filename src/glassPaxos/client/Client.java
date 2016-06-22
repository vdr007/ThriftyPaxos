package glassPaxos.client;

import glassPaxos.Configuration;
import glassPaxos.SimpleLogger;
import glassPaxos.interfaces.AppClientCallback;
import glassPaxos.interfaces.LibClientInterface;
import glassPaxos.network.EventHandler;
import glassPaxos.network.NettyNetwork;
import glassPaxos.network.Network;
import glassPaxos.network.NodeIdentifier;
import glassPaxos.network.messages.*;
import glassPaxos.utils.RequestEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class Client implements EventHandler, LibClientInterface {

	private NodeIdentifier myID;
	private Network network;
    private long localSeqNum = 1;
    private boolean localSeqNumSync = false;
    private SimpleLogger LOG;
    // private CopyOnWriteArrayList<RequestEntry> pendingRequests;
    // private LinkedBlockingQueue<RequestEntry> outgoingRequests;
    private final LinkedList<RequestEntry> pendingRequests = new LinkedList<RequestEntry>();
    private final LinkedList<RequestEntry> outgoingRequests = new LinkedList<RequestEntry>();
    private final LinkedBlockingQueue<RequestReplyMessage> pendingReply =
            new LinkedBlockingQueue<RequestReplyMessage>(500);
    // private ConcurrentHashMap<Integer, Long> pingTracks;

    private int leaderIdx = 0;
    private NodeIdentifier leaderID = null;
    private final Object lockLeader = new Object();
    private int acceptorMaxRound = 0;
    private int round = 0;

    private int finished = 0;
    private int lastFinished = 0;
    private double totalLatency = 0.0; //s
    private final Object cnt_lock = new Object();

    private Timer qrTimer;
    private long qrInterval;
    private Timer pingTimer;
    private long pingInterval;
    private long startTimestamp;

    //private long tsLeader = 0L;
    private ArrayList<NodeIdentifier> activeAcceptors = new ArrayList<NodeIdentifier>();
    private boolean checkLeader = false;

    final SendThread sendThread = new SendThread(outgoingRequests, pendingRequests, "sendThread");
    final EventThread eventThread = new EventThread(pendingRequests, "eventThread");
    private boolean isRunning = true;
    //private final Object pendingRequestLock = new Object();
    //private long pendingRequestCount = 0;

    public Client(NodeIdentifier myID){
		this.myID = myID;
        /* must be disable, or else run into error */
        //this.network = new NettyNetwork(myID, this);
    }

    public Client() {
    }

    /*
    private synchronized int getLastFinished() {
        return lastFinished;
    }

    private synchronized void setLastFinished(int newValue) {
        lastFinished = newValue;
    }

    public synchronized int getFinished() {
        return finished;
    }

    public synchronized void addFinished() {
        finished += 1;
    }
    */

    public void initEnv(int hostRank, String configFile) throws Exception{
        Configuration.initConfiguration(configFile);

		Configuration.addActiveLogger("NettyNetwork", SimpleLogger.INFO);
		Configuration.addActiveLogger("Client", Configuration.debugLevel);
        LOG = SimpleLogger.getLogger("Client");

        myID = Configuration.clientIDs.get(hostRank);
        network = new NettyNetwork(myID, this);

        // pendingRequests = new CopyOnWriteArrayList<>();
        // outgoingRequests = new LinkedBlockingQueue<>();
        // pingTracks = new ConcurrentHashMap<>();

        qrInterval = Configuration.showPerformanceInterval * 100;
        pingInterval = Configuration.pingTimeout/2;
        qrTimer = new Timer();
        qrTimer.scheduleAtFixedRate(new QrRecordTimer(), 0, qrInterval);
        startTimestamp = System.currentTimeMillis();


        for (int i : Configuration.acceptorIDs.keySet()) {
            activeAcceptors.add(Configuration.acceptorIDs.get(i));
        }
        //locateLeader();
        if (!activeAcceptors.isEmpty()) {
            naiveLocateLeader();
        }
        NodeIdentifier fakeLeader = leaderID;
        while (fakeLeader == null) {
            Thread.sleep(100);
            fakeLeader = leaderID;
        }
        LOG.info("locate leader succeed %s\n", leaderID);

        boolean syncFinished = localSeqNumSync;
        while (!syncFinished) {
            network.sendMessage(leaderID, new SyncMessage(myID, 0, 0));
            Thread.sleep(100);
            syncFinished = localSeqNumSync;
        }
        LOG.debug("get localSeqNum sync succeed %d\n", localSeqNum);
        Thread.sleep(400);

        eventThread.start();
        sendThread.start();
        LOG.info("start eventThread and sendThread\n");

    }

    private void naiveLocateLeader() {
        if (activeAcceptors.isEmpty()) {
            for (NodeIdentifier acc : Configuration.acceptorIDs.values()) {
                network.sendMessage(acc, new PingMessage(myID, 0));
            }
            LOG.warning("naiveLocateLeader activeAcc=empty send ping => %s\n", Configuration.acceptorIDs.values());
        } else {
            for (NodeIdentifier acc : activeAcceptors) {
                network.sendMessage(acc, new PingMessage(myID, 0));
            }
            LOG.debug("naiveLocateLeader send ping => active=%s\n", Arrays.toString(activeAcceptors.toArray()));
        }
    }

    private void locateLeader() {
        leaderIdx = (leaderIdx+1)%(Configuration.numAcceptors);
        /* correct index since our acceptorIDs starts from 1,2,...*/
        if (leaderIdx == 0)
            leaderIdx = Configuration.numAcceptors;
        NodeIdentifier lid = Configuration.acceptorIDs.get(leaderIdx);
        //tsLeader = System.currentTimeMillis();
        LOG.debug("locateLeader send PingMessage myID %s ==> %s\n", myID, lid);
        PingMessage ping = new PingMessage(myID, 0);
        network.sendMessage(lid, ping);
    }

    @Deprecated
    private void processRequestReply(RequestReplyMessage msg) {

        long replyReqNum = msg.getReqNum();
        RequestEntry re;
        synchronized (pendingRequests) {
            if (!pendingRequests.isEmpty()) {
                re = pendingRequests.get(0);
                //LOG.debug("receive rr: %s, pendingReq: %s\n", msg.toString(), re.toString());
                if (re.getReqnum() == replyReqNum) {
                    pendingRequests.remove(0);
                    //addFinished();
                    synchronized (cnt_lock) {
                        finished += 1;
                    }

                    LOG.debug("request[%d] complete\n", re.getReqnum());
                    re.getCb().requestComplete(re.getReq(), true, msg.getReply());

                    // cleanup consecutive pending replies
                    while ((!pendingRequests.isEmpty()) && pendingRequests.get(0) != null &&
                            pendingRequests.get(0).getcTime() == 0) {
                        RequestEntry del = pendingRequests.remove(0);
                        del.getCb().requestComplete(del.getReq(), true, del.getReply());
                        //addFinished();
                        synchronized (cnt_lock) {
                            finished += 1;
                        }
                        LOG.debug("=>request[%d] complete\n", del.getReqnum());
                    }
                } else if (re.getReqnum() < replyReqNum) {
                    //only count the first reply for the same request
                    LOG.debug(" >>> [pending# %d, reply# %d]\n",
                            re.getReqnum(), replyReqNum);
                    Iterator<RequestEntry> it = pendingRequests.iterator();
                    int indexOfReply = 0;
                    while (it.hasNext()) {
                        RequestEntry pending = it.next();
                        if (pending.getReqnum() == replyReqNum) {
                            // set cTime=>0 to indicate request reply received
                            pending.setcTime(0);
                            pending.setReply(msg.getReply());
                            //// pendingRequests.set(indexOfReply, pending); //todo NOT NEEDED Java reference copy
                            LOG.debug("=>pending# %d, reply# %d, set requestReply[%d]: %s\n",
                                    re.getReqnum(), replyReqNum, indexOfReply, pending);
                            break;
                        }
                        indexOfReply++;
                    }

                } else {
                /* drop already processed reply */
                }

                //synchronized (pendingRequestLock) {
                //    pendingRequestCount--;
                //}
            }
        }
    }

    private void processPing(PingMessage msg) {
        //pingTracks.put(nodeID.hashCode(), System.currentTimeMillis());

        NodeIdentifier nodeID = msg.getSender();
        switch (nodeID.getRole()) {
            case ACCEPTOR:
                synchronized (activeAcceptors) {
                    if (!activeAcceptors.contains(nodeID)) {
                        activeAcceptors.add(nodeID);
                        LOG.info("add %s into activeAcceptors %s\n", nodeID, Arrays.toString(activeAcceptors.toArray()));
                    }
                }
                //tsLeader = System.currentTimeMillis();
                int msgRound = (int)msg.getInfo();
                //if (hash != 0 && (null == leaderID || (leaderID.hashCode() != hash))) {
                synchronized (lockLeader) {
                    if (msgRound > acceptorMaxRound) {
                        int accID = msgRound % Configuration.numAcceptors;
                        if (accID == 0)
                            accID = Configuration.numAcceptors;
                        leaderID = new NodeIdentifier(NodeIdentifier.Role.ACCEPTOR, accID);
                        acceptorMaxRound = msgRound;
                        //tsLeader = 0L;
                        LOG.info("client get new leader %s, checkLeader=%b\n", leaderID, checkLeader);
                    }
                    if (leaderID != null) {
                        checkLeader = false;
                        LOG.debug("reset checkLeader=%b <= %s\n", checkLeader, msg);
                    }
                }
                break;
            // No PingMessage exchange between clients and learners
            //case LEARNER: //todo keep learner tracks and use it in handleFailure
            //    break;
            default: //no inter-client pings
                LOG.debug("receive unexpected ping message from %s\n", nodeID);
                break;
        }
    }

    private void processSyncClientReply(SyncClientReplyMessage msg) {
        LOG.debug("get %s, localSeqNum %d\n", msg, localSeqNum);
        localSeqNum = Math.max(localSeqNum, msg.getLastRequestNum()+1);
        localSeqNumSync = true;
    }

    private void processInfo(InfoMessage info) {
        double tm = (System.currentTimeMillis()-startTimestamp)/1000.0;
        System.out.printf("=>%s ts(s)=%.0f xDim=%.1f\n",
                info, tm, tm*10/Configuration.showPerformanceInterval);
        System.out.flush();
    }

    @Override
    public void sendRequest(byte[] req, AppClientCallback callback) {
        synchronized (outgoingRequests) {
            while (outgoingRequests.size() >= Configuration.maxOutgoingRequests) {
                try {
                    outgoingRequests.wait();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            //set initTime here, includes request queuing time
            outgoingRequests.add(new RequestEntry(0, req, false, 0, System.nanoTime(), callback, null));
            outgoingRequests.notifyAll();
            //LOG.info("add request to outgoingRequests size=%d pending=%d < max=%d\n",
            //        outgoingRequests.size(), pendingRequestCount, Configuration.maxPendingRequests);
        }
    }

    /*
    public void oldSendRequest(byte[] req, AppClientCallback callback) {
        if (outgoingRequests.size() < Configuration.maxOutgoingRequests) {
            try {
                outgoingRequests.put(new RequestEntry(0, req, false, 0, callback, null));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if (leaderID == null) {
            try {
                Thread.sleep(50); //sleep 50ms before locate new leader
                LOG.debug("sleep 50ms and locate new leader\n");
                naiveLocateLeader();
                return;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if (pendingRequests.size() < Configuration.maxPendingRequests) {
            try {
                RequestEntry out = outgoingRequests.take();
                RequestEntry re = new RequestEntry(localSeqNum, out.getReq(),
                        false, System.currentTimeMillis(), out.getCb(), null);
                pendingRequests.add(re);
                //LOG.debug("add [%d: %s]\n", pendingRequests.indexOf(re), re);

                RequestMessage requestMsg =
                        new RequestMessage(myID, localSeqNum, out.getReq());
                network.sendMessage(leaderID, requestMsg);
                //LOG.debug("ClientLib send %s to [%s]\n", requestMsg.toString(), leaderID);
                localSeqNum = localSeqNum + 1;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    */

	@Override
	public void handleMessage(Message msg) {
        switch (Message.MSG_TYPE.values()[msg.getType()]) {
            case REQ_REPLY:
                // processRequestReply((RequestReplyMessage) msg);
                try {
                    pendingReply.put((RequestReplyMessage) msg);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                break;
            case PING:
                processPing((PingMessage) msg);
                break;
            case SYNC_CLIENT_REPLY:
                processSyncClientReply((SyncClientReplyMessage) msg);
                break;
            case INFO:
                processInfo((InfoMessage) msg);
                break;
            default:
                LOG.debug("client get unknown msg " + msg);
                throw new RuntimeException("client received unsupported message");
        }
	}

	@Override
	public void handleTimer() {

        /* scan for ping timeout */
        synchronized (lockLeader) {
            if ((checkLeader || leaderID == null) /* && !activeAcceptors.isEmpty() && tsLeader!=0 &&
                System.currentTimeMillis() - tsLeader > Configuration.pingTimeout*/) {
                synchronized (activeAcceptors) {
                    LOG.info("locate leader checkLeader=%b\n", checkLeader);
                    naiveLocateLeader();
                }
                return;
            }
        }

        synchronized (pendingRequests) {
        /* scan timeout requests */
            Iterator<RequestEntry> it = pendingRequests.iterator();
            //int j = 0;
            while (it.hasNext()) {
                RequestEntry re = it.next();
                //LOG.debug("handleTimer check[%d/%d] %s\n",
                //        j, pendingRequests.size(), re);
                //j++;
                if (re.getcTime() == 0) {
                    continue;
                } else if (System.currentTimeMillis() - re.getcTime() > Configuration.requestTimeout) {
                    //checkLeader = true; //todo should enable
                    //resubmit proposal
                    //// int idx = pendingRequests.indexOf(re);
                    re.setResubmit(true);
                    re.setcTime(System.currentTimeMillis());
                    //// pendingRequests.set(idx, re); //todo NOT NEEDED Java copy by reference
                    NodeIdentifier leader;
                    synchronized (lockLeader) {
                        leader = leaderID;
                    }
                    LOG.debug("timeout resubmit %s\n", re);

                    if (leader != null) {
                        network.sendMessage(leader, new RequestMessage(myID, re.getReqnum(), re.getReq()));
                    }

                } else if (!re.isResubmit()) {
                    //LOG.debug("break the for loop\n");
                    break; //monotonic increasing creation time for requests
                }
            }
        }
	}

    public class QrRecordTimer extends TimerTask {
        double rps, lat;
        int done_sd, sdFinished, sdLastFinished;

        @Override
        public void run() {
            //sdFinished = getFinished();
            synchronized (cnt_lock) {
                sdFinished = finished;
                sdLastFinished = lastFinished;
                lastFinished = finished;
                lat = totalLatency;
                totalLatency = 0.0; //reset
            }
            try {
                //done_sd = sdFinished - getLastFinished();
                done_sd = sdFinished - sdLastFinished;
                if (done_sd > 0) {
                    rps = (double) 1000 * done_sd / qrInterval;
                    lat = lat * 1000.0 / done_sd;
                } else {
                    rps = 0.0;
                    lat = 0.0;
                }

                //System.out.println("# TimerTask round " + round + " LAST: "
                //        + getLastFinished() + " CURRENT: " + sdFinished
                //+ " time (s): "+ qrInterval/1000 +" TPS: "+ String.format("%.1f",tps));
                System.out.printf("#round=%d last=%d current=%d interval=%d lat(ms)=%.3f rps=%.0f\n",
                        round, sdLastFinished, sdFinished, qrInterval/1000, lat, rps);

            } catch (Exception e) {
                LOG.debug("quorum Error while write to LOG file", e);
            }
            round++;
            //setLastFinished(sdFinished);
            /* detailed stats
            ListIterator<RequestReplyMessage> it = pendingReplies.listIterator();
            while (it.hasNext()) {
                LOG.info("pending reply: %s\n", it.next());
            }
            Iterator<RequestEntry> it = pendingRequests.iterator();
            while (it.hasNext()) {
                LOG.info("pending request: %s\n", it.next());
            }
            */
        }
    }

    private HashMap<String, Object> properties = new HashMap<String, Object>();
    public void setProperty(String key, Object val){
    	properties.put(key, val);
    }
    
    public Object getProperty(String key){
    	return properties.get(key);
    }

	@Override
	public void handleFailure(NodeIdentifier node, Throwable cause) {
        if (cause instanceof ClosedChannelException || cause instanceof IOException) {
            LOG.warning("catch %s ClosedChannelException or IOException\n", node);
            synchronized (activeAcceptors) {
                if (activeAcceptors.contains(node)) {
                    int idx = activeAcceptors.indexOf(node);
                    activeAcceptors.remove(idx);
                    LOG.info("remove %s from activeAcceptors, remain %s\n", node,
                            Arrays.toString(activeAcceptors.toArray()));
                }
            }
            synchronized (lockLeader) {
                if (leaderID != null && leaderID.equals(node)) {
                    leaderID = null;
                    checkLeader = true;
                    naiveLocateLeader();
                }
            }
        }
	}

    private class SendThread extends Thread {
        private Queue<RequestEntry> recvQueue;
        private Queue<RequestEntry> sendQueue;

        public SendThread(Queue<RequestEntry> rq, Queue<RequestEntry> sq, String name) {
            super(name);
            this.recvQueue = rq;
            this.sendQueue = sq;
        }

        public void run(){
            while(isRunning){
                synchronized (lockLeader) {
                    if (leaderID == null) {
                        try {
                            Thread.sleep(100); //sleep 100ms before locate new leader
                            //System.out.printf("SendThread sleep 20ms and locate new leader\n");
                            naiveLocateLeader();
                            //return;
                            continue;
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
                //System.out.printf("SendThread is running\n");
                synchronized (recvQueue) {
                    while (recvQueue.isEmpty()) {
                        try {
                            recvQueue.wait();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    RequestEntry out = recvQueue.remove();
                    RequestEntry re = new RequestEntry(localSeqNum, out.getReq(), false,
                            System.currentTimeMillis(), out.getInitTime(), out.getCb(), null);

                    synchronized (sendQueue) {
                        while (sendQueue.size() >= Configuration.maxPendingRequests) {
                            try {
                                sendQueue.wait();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }

                        sendQueue.add(re);
                        //LOG.debug("add [%d: %s]\n", pendingRequests.indexOf(re), re);

                        RequestMessage requestMsg = new RequestMessage(myID, localSeqNum, out.getReq());
                        NodeIdentifier leader;
                        synchronized (lockLeader) {
                            leader = leaderID;
                        }
                        if (leader != null) {
                            network.sendMessage(leader, requestMsg);
                        }

                        //LOG.debug("ClientLib send %s to [%s]\n", requestMsg.toString(), leaderID);
                        localSeqNum = localSeqNum + 1;

                        sendQueue.notifyAll();
                    }

                    recvQueue.notifyAll();
                }
            }
        }
    }

    private class EventThread extends Thread {
        private Queue<RequestEntry> eventQueue;

        public EventThread(Queue<RequestEntry> eq, String name) {
            super(name);
            this.eventQueue = eq;
        }

        private void processReply(RequestReplyMessage msg) {
            long replyReqNum = msg.getReqNum();
            RequestEntry re;
            synchronized (eventQueue) {
                if (!eventQueue.isEmpty()) {
                    re = eventQueue.peek();
                    //LOG.debug("receive rr: %s, pendingReq: %s\n", msg.toString(), re.toString());
                    if (re.getReqnum() == replyReqNum) {
                        eventQueue.remove();
                        re.getCb().requestComplete(re.getReq(), true, msg.getReply());
                        LOG.debug("request[%d] complete\n", re.getReqnum());

                        //addFinished();
                        synchronized (cnt_lock) {
                            finished += 1;
                            totalLatency += (System.nanoTime()-re.getInitTime())/1000000000.0; //sec
                        }

                        // cleanup consecutive pending replies
                        while ((!eventQueue.isEmpty()) && eventQueue.peek() != null &&
                                eventQueue.peek().getcTime() == 0) {
                            RequestEntry del = eventQueue.remove();
                            del.getCb().requestComplete(del.getReq(), true, del.getReply());
                            //addFinished();
                            synchronized (cnt_lock) {
                                finished += 1;
                                totalLatency += (System.nanoTime()-re.getInitTime())/1000000000.0; //sec
                            }
                            LOG.debug("=>request[%d] complete\n", del.getReqnum());
                        }
                        eventQueue.notifyAll();

                    } else if (re.getReqnum() < replyReqNum) {
                        //only count the first reply for the same request
                        LOG.debug(" >>> [pending# %d, reply# %d]\n", re.getReqnum(), replyReqNum);

                        Iterator<RequestEntry> it = eventQueue.iterator();
                        int indexOfReply = 0;
                        while (it.hasNext()) {
                            RequestEntry pending = it.next();
                            if (pending.getReqnum() == replyReqNum) {
                                // set cTime=>0 to indicate request reply received
                                pending.setcTime(0);
                                pending.setReply(msg.getReply());
                                ////pendingRequests.set(indexOfReply, pending); // todo NOT NEEDED
                                LOG.debug("=>pending#=%d < reply#=%d, set reply#=%d %s\n", re.getReqnum(),
                                        replyReqNum, indexOfReply, pending);
                                break;
                            }
                            indexOfReply++;
                        }

                    } else {/* drop already processed reply */
                    }
                }
            }
        }

        public void run(){
            while (isRunning) {
                //System.out.printf("EventThread is running\n");
                try {
                    //System.out.printf("EventThread will process waitingEvents\n");
                    Object event = pendingReply.take();
                    processReply((RequestReplyMessage) event);

                } catch (InterruptedException e) {
                    LOG.error("Event thread exiting due to interruption", e);
                }
            }
        }
    }

    private void modifyTest(Queue<RequestEntry> queue) {
        Iterator<RequestEntry> it = queue.iterator();
        RequestEntry nre;
        int k = 1000;
        while (it.hasNext()) {
            nre = it.next();
            nre.setcTime(k);
            nre.setResubmit(true);
            k += 100;
        }
    }

    public static void main(String []args) throws Exception {
        Client client = new Client();

        LinkedList<RequestEntry> testList = new LinkedList<RequestEntry>();
        for (int i=1; i<4; i++) {
            String str = "hello world " + i;
            byte[] value = str.getBytes();
            byte[] req = new byte[1 + 8 + value.length];
            ByteBuffer b = ByteBuffer.wrap(req);
            b.put((byte) 1);
            b.putLong((long)i);
            b.put(value);
            RequestEntry re =  new RequestEntry(i, req, false, 0, 0, null, null);
            testList.add(re);
            System.out.printf("idx=%d %s\n", i, re);
        }

        client.modifyTest(testList);

        Iterator<RequestEntry> iter = testList.iterator();
        RequestEntry entry;
        while (iter.hasNext()) {
            entry = iter.next();
            System.out.printf("==> %s\n", entry);
        }
    }
}
