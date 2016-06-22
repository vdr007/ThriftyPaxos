package glassPaxos.learner;

import glassPaxos.Configuration;
import glassPaxos.SimpleLogger;
import glassPaxos.apps.hashtable.HashTableServer;
import glassPaxos.interfaces.AppServerInterface;
import glassPaxos.interfaces.LearnerLogCbInterface;
import glassPaxos.interfaces.LibServerInterface;
import glassPaxos.interfaces.Request;
import glassPaxos.network.EventHandler;
import glassPaxos.network.NettyNetwork;
import glassPaxos.network.Network;
import glassPaxos.network.NodeIdentifier;
import glassPaxos.network.messages.*;
import glassPaxos.storage.LearnerLog;
import glassPaxos.utils.DecisionCountProfiler;
import org.apache.commons.configuration.ConfigurationException;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Learner implements EventHandler, LibServerInterface, LearnerLogCbInterface {
	private NodeIdentifier myID = null;
	private Network network = null;
    private static SimpleLogger LOG;
    private boolean learnerIsActive = true;
    private long firstUnstable;
    private long firstHole;  //first decision hole for learners
    private long maxReceived;
    private long checkPoint;
    //private long catchupFirstUnstable;

    private static int maxPendingCatchup;
    private static int syncChunkSize;
    private static int ckPtInterval;
    private static int maxPendingReq;

    private final TreeMap<Long, DecisionMessage> pendingDecisions = new TreeMap<Long, DecisionMessage>();
    private long lastPDCleanup = 0L;
    private static int maxPDCacheSize; //default 400k

    private final HashMap<Integer, TreeMap<Long, byte[]>> replyCache =
            new HashMap<Integer, TreeMap<Long, byte[]>>();

    private final HashMap<Integer, Long> replyCacheLastChecked =
            new HashMap<Integer, Long>();

    //K: snapshotIndex, TreeMap<token, chunk>
    private final TreeMap<Integer, SnapshotCachedChunk> catchupSnapshotSendCached =
            new TreeMap<Integer, SnapshotCachedChunk>();
    private final TreeMap<Integer, SyncSnapshotMessage> learnerSyncSSMsgBuf = new TreeMap<Integer, SyncSnapshotMessage>();
    //private final LinkedList<Integer> leaSyncTrack = new LinkedList<Integer>();

    //learner catchup snapshot
    private NodeIdentifier catchupLearner;
    private NodeIdentifier lastPingLearner;
    private long catchupSnapshotIdx = 0;
    private int catchupSSExpected = 0; //expected next snapshot chunk offset
    private int catchupSSStart = 0; //snapshot chunk next start offset
    private boolean catchupSnapshotSendDone = false;
    private boolean catchupSnapshotFinish = true;
    private boolean catchupSnapshotDone = true;
    private boolean inLoadSnapshot = false;
    private boolean inTakeSnapshot = false;
    private double slackRatioSS = 1.0;
    private final TreeMap<Integer, Long> catchupSnapshotRecTrack = new TreeMap<Integer, Long>();
    private LearnerLog learnerLog;
    private LearnerLog catchupLeaLog;
    private Iterator<DecisionMessage> leaLogIterator = null;
    private long leaLogIteratorPos;
    private boolean leaLogCatchupDone = true;
    private boolean isDynSSThresholdEnabled = true;
    private boolean useAvgMethod = true;
    private int catchupSSThreshold;
    private int lastSSChunkUnstable = 1;

    private long lastLeaLogReadIndex;
    private int catchupLogThreshold;
    private boolean enableDynLogThreshold = true;

    //learner catchup decisions
    private long catchupDecisionStart = 0;
    private long catchupDecStartBackup;
    private long catchupDecisionEnd = 0;
    private boolean catchupDecisionSendDone = false;
    private boolean catchupDecisionFinish = true;
    private boolean catchupDecisionDone = true;
    private final TreeMap<Long, DecisionChunk> catchupDecisionTrack = new TreeMap<Long, DecisionChunk>();

    // used by catchupSnapshot sender side, todo optimize
    private long lastBatchOpTime;
    private long batchOpTimeout;

    private List<byte[]> catchupSendTokens = new LinkedList<byte[]>();
    private ListIterator<byte[]> catchupSendTokensIterator;
    private int tokenIteratorPos = 0;

    //track ping footprint <nodeHashCode, timestamp>
    private final ConcurrentHashMap<Integer, Long> pingLearnerTracks = new ConcurrentHashMap<Integer, Long>();
    private final ConcurrentHashMap<Integer, Long> pingAcceptorTracks = new ConcurrentHashMap<Integer, Long>();

    private Timer pingTimer;
    private long pingInterval;

    private NodeIdentifier leaderID = null;

    private AppServerInterface appServerInterface;

    private double catchupSnapshotTime;          // accumulated pure time for store snapshot on receiver
    private long catchupSSRecTime;               // total time for catchup snapshot on receiver
    private double catchupAssembleSnapshotTime;  // accumulated pure time for fetching snapshot on sender
    private long catchupSSSendTime;              // total time for catchup snapshot on sender
    private double catchupPDecisionTime;         // accumulated pure time for catchup decisions on receiver
    private long catchupPDRecTime;               // total time for catchup decisions on receiver

    private final DecisionCountProfiler decCount = new DecisionCountProfiler();
    private long decCountCycle = 0L;

    private boolean enableDecInParallel;

    public Learner() {}

    @Deprecated
    public Learner(NodeIdentifier id) {
        this.myID = id;
        network = new NettyNetwork(myID, this);
    }

    @Override
    public void sendReply(Request req, byte[] reply) {
    }

    @Override
    public void takeSnapshotComplete(long index) {
        if (index > checkPoint) {
            long tmA = System.nanoTime();

            FreeLogMessage freeLog;
            synchronized (pendingDecisions) {
                checkPoint = index;
                pendingDecisions.headMap(checkPoint + 1).clear();
                inTakeSnapshot = false;
                freeLog = new FreeLogMessage(myID, checkPoint, 0);
            }

            if (pingAcceptorTracks.isEmpty()) {
                LOG.error("error pingAcceptorTracks is empty\n");
                throw new RuntimeException("pingAcceptorTracks is empty\n");
            } else {
                for (int k : pingAcceptorTracks.keySet()) {
                    NodeIdentifier acceptor = new NodeIdentifier(k);
                    network.sendMessage(acceptor, freeLog);
                }
                LOG.info("learner send %s => %s firstUn=%d\n", freeLog, pingAcceptorTracks.keySet(), firstUnstable);
                long tmB = System.nanoTime();
                LOG.info("takeSnapshotComplete pdSz %d, ckPt %d, firstUnstable %d, freeCache %.2f ms\n",
                        pendingDecisions.size(), checkPoint, firstUnstable, (tmB - tmA) / 1000000.0);
            }
        } else {
            synchronized (pendingDecisions) {
                inTakeSnapshot = false;
            }
            LOG.warning("warning takeSnapshotComplete index.%d <= ckPt.%d\n", index, checkPoint);
        }
    }

    @Override
    public void loadSnapshotComplete(long index) {
        if (index > checkPoint) {
            synchronized (pendingDecisions) {
                checkPoint = Math.max(index, checkPoint);
                firstUnstable = Math.max(firstUnstable, checkPoint + 1);
                maxReceived = Math.max(maxReceived, checkPoint);
                firstHole = Math.max(firstHole, checkPoint+1);
                pendingDecisions.headMap(checkPoint + 1).clear();
                inLoadSnapshot = false;
                FreeLogMessage freeLog = new FreeLogMessage(myID, checkPoint, 0);
                for (int k : pingAcceptorTracks.keySet()) {
                    NodeIdentifier acceptor = new NodeIdentifier(k);
                    network.sendMessage(acceptor, freeLog);
                    LOG.info("=>learner send %s=>%s\n", freeLog, acceptor);
                }
            }
            LOG.info("loadSnapshotComplete pdSz %d, ckPt %d, firstUn %d, inLoadSnapshot=%b\n",
                    pendingDecisions.size(), checkPoint, firstUnstable, inLoadSnapshot);
        } else {
            LOG.warning("warning loadSnapshotComplete index.%d <= ckPt.%d\n", index, checkPoint);
        }
    }

    private class DecisionChunk {
        public DecisionChunk(int len, long ts /*, byte[] dChunk*/) {
            this.len = len;
            this.ts = ts;
            //this.dChunk = dChunk;
        }

        int len;
        long ts;
        //byte[] dChunk;
    }

    private class SnapshotCachedChunk {
        public SnapshotCachedChunk() {
            this.token = null;
            this.chunk = null;
            this.timestamp = System.currentTimeMillis();
        }

        public SnapshotCachedChunk(byte[] token, byte[] chunk) {
            this.token = token;
            this.chunk = chunk;
            this.timestamp = System.currentTimeMillis();
        }

        long timestamp;
        byte[] token;
        byte[] chunk;
    }

    public void start(int hostRank, String config) throws ConfigurationException {
        Configuration.initConfiguration(config);

        myID = Configuration.learnerIDs.get(hostRank);
		network = new NettyNetwork(myID, this);

		Configuration.addActiveLogger("NettyNetwork", Configuration.debugLevel);
		Configuration.addActiveLogger("Learner", Configuration.debugLevel);
        Configuration.addActiveLogger("LearnerLog", SimpleLogger.DEBUG);
        LOG = SimpleLogger.getLogger("Learner");

        for (int i : Configuration.clientIDs.keySet()) {  //i=1,2,3...
            replyCache.put(i, new TreeMap<Long, byte[]>());
            replyCacheLastChecked.put(i, 0L);
        }

        for (int i : Configuration.learnerIDs.keySet())
            pingLearnerTracks.put(Configuration.learnerIDs.get(i).hashCode(), System.currentTimeMillis());

        for (int i : Configuration.acceptorIDs.keySet())
            pingAcceptorTracks.put(Configuration.acceptorIDs.get(i).hashCode(), System.currentTimeMillis());

        LOG.info("initial pingLearnerTracks %s, pingAcceptorTracks %s\n",
                pingLearnerTracks.keySet(), pingAcceptorTracks.keySet());

        checkPoint = appServerInterface.getLatestSnapshotIndex();
        maxReceived = checkPoint;
        firstUnstable = checkPoint + 1;
        firstHole = checkPoint + 1;
        LOG.info("init ckPt.%d, maxRec.%d, firstUn.%d, hole.%d\n", checkPoint, maxReceived, firstUnstable, firstHole);

        catchupSnapshotTime = 0L;
        catchupPDecisionTime = 0L;
        catchupAssembleSnapshotTime = 0L;

        maxPDCacheSize = Configuration.maxLogCacheSize;
        enableDecInParallel = Configuration.enableDecInParallel;

        batchOpTimeout = Configuration.learnerTimeout/5;  //todo: change later
        catchupSSThreshold = Configuration.catchupSnapshotThrottle;
        catchupLogThreshold = Configuration.catchupLeaLogThreshold;
        LOG.info("catchup threshold snapshot=%d log=%d maxPDCacheSize=%d enableDecInParallel=%b\n", catchupSSThreshold,
                catchupLogThreshold, maxPDCacheSize, enableDecInParallel);

        maxPendingCatchup = Configuration.maxPendingCatchupSync;
        syncChunkSize = Configuration.maxSyncDiff;
        ckPtInterval = Configuration.checkpointInterval;
        maxPendingReq = Configuration.maxPendingRequests;

        learnerLog = new LearnerLog(Configuration.learnerLogDir, this);
        catchupLeaLog = new LearnerLog(Configuration.catchupLeaLogDir, this);

        pingTimer = new Timer();
        pingInterval = Configuration.pingTimeout/4;
        pingTimer.scheduleAtFixedRate(new PingTimer(), 0, pingInterval);

        locateLeader();
	}

    public void setApplicationServer(AppServerInterface app) {
        appServerInterface = app;
    }

    private void locateLeader() {
        for (NodeIdentifier acc : Configuration.acceptorIDs.values()) {
            network.sendMessage(acc, new PingMessage(myID, -5));
        }
        LOG.info("locateLeader ping(-5) => %s\n", Configuration.acceptorIDs.values());
    }

    private void showReqCountInfo() {
        LOG.info(decCount.displayCounterChange());
        decCount.updateLastCounters();
    }

    private void updateMultiStates(long from, long to) {
        //[from, to] range might have holes
        //LOG.debug("<== %s enter updateStates\n", myID);
        SortedMap<Long, DecisionMessage> sMap = pendingDecisions.subMap(from, to + 1);

        long trackIndex = from;
        for (Map.Entry<Long, DecisionMessage> entry : sMap.entrySet()) {
            /* method1: with padding entries */
            //if (entry.getValue() != null)

            /* method2: no padding */
            if (entry.getValue().getIndex() == trackIndex) {
                updateState(entry.getValue());
            } else {
                /* stop immediately if detect holes
                 * method1: makeup entry<key, null>
                 * method2: not exist
                 */
                break;
            }
            trackIndex++;
        }
    }

    private void updateState(DecisionMessage msg) {
        // if (parallelSendDecision) {
        //     LOG.info("get parallel %s\n", msg);
        // }
        firstUnstable++;

        ClientRequest cReq;
        byte[] requests;
        requests = msg.getRequest();
        ByteBuffer bb = ByteBuffer.wrap(requests);
        int numReq = bb.getInt();
        int reqID;
        byte[] req;
        for (int i = 0; i < numReq; i++) {
            req = new byte[bb.getInt()];
            bb.get(req);
            cReq = new ClientRequest();
            cReq.readFromRequestByteArray(req);

            if (cReq.getReqnum() == 0) {
                decCount.addNoop();
                LOG.debug("get NO-OP request %s\n", msg);
                return;
            }
            reqID = cReq.getClientID(); //reqID=1,2,3...
            Request wrapperReq = new Request();
            wrapperReq.request = cReq.getRequest(); //todo batching check this
            wrapperReq.client = new NodeIdentifier(NodeIdentifier.Role.CLIENT, reqID);
            wrapperReq.clientSeqID = cReq.getReqnum();
            byte[] requestReply = appServerInterface.executeRequest(wrapperReq);
            /* replyCache add new entry IFF client remove received one
            * e.g. client remove req_1 and accept req_301, learner receive req_301
            * and put it into slot (301 % 300), assuming maxPendingReq=300
            * int reqHash = (int) ((msg.getReqNum()) % maxPendingReq);
            * int rcKey = (msg.getReqID() << 24) | reqHash;
            */
            if (replyCache.get(reqID) == null)
                throw new RuntimeException("error, replyCache does not have entry " + reqID);

            replyCache.get(reqID).put(cReq.getReqnum(), requestReply);

            //todo: firstUnstable might be more accurate?
            //if (firstUnstable > replyCacheLastChecked + 2 * maxPendingReq) {
            if (replyCache.get(reqID).size() > 2 * maxPendingReq) {
                long nextChecked = replyCacheLastChecked.get(reqID) + maxPendingReq;
                replyCacheLastChecked.put(reqID, nextChecked);
                replyCache.get(reqID).headMap(nextChecked + 1).clear();
                LOG.debug("replyCache entry[%d] remaining %d\n", reqID, replyCache.get(reqID).size());
            }

            decCount.addExec();
            if (leaLogCatchupDone) {
                RequestReplyMessage rr =
                        new RequestReplyMessage(myID, cReq.getReqnum(), requestReply);

                network.sendMessage(wrapperReq.client, rr);
                LOG.debug("update_state send %s => %s\n", rr, wrapperReq.client);
                // if (0 == msg.getReqNum() % 200000) {
                //     LOG.info("leaLogCatchupDone=%b reqID=%d\n", leaLogCatchupDone, reqID);
                // }
                //} else {
                // if (0 == msg.getReqNum() % 200000) {
                //     LOG.info("skip sendRR leaLogCatchupDone=%b reqID=%d\n", leaLogCatchupDone, reqID);
                // }
            }
        }
    }

    private void processDecision(DecisionMessage msg) {

        long index = msg.getIndex();
        //*//pingAcceptorTracks.put(msg.getSender().hashCode(), System.currentTimeMillis());
        byte[] reqStr;
        ClientRequest cr;
        int reqID;
        long reqNum;

        ByteBuffer bb = ByteBuffer.wrap(msg.getRequest());
        int numRequests = bb.getInt();
        // for (int i = 0; i < numRequests; i++) {
        //    reqStr = new byte[bb.getInt()];
        //    bb.get(reqStr);
        //    cr = new ClientRequest();
        //    cr.readFromRequestByteArray(reqStr);

        //    reqID = cr.getClientID();
        //    reqNum = cr.getReqnum();
        //}

        if (index == 0) { /* resubmit request, decision made already */

            if (numRequests != 1) {
                LOG.warning("pDec resubmit request number=%d\n", numRequests);
                return;
            }

            for (int i = 0; i < numRequests; i++) {
                reqStr = new byte[bb.getInt()];
                bb.get(reqStr);
                cr = new ClientRequest();
                cr.readFromRequestByteArray(reqStr);
                decCount.addResubmit();
                reqNum = cr.getReqnum();
                reqID = cr.getClientID();
                if (replyCache.get(reqID) != null &&
                        replyCache.get(reqID).get(reqNum) != null) {
                    // 1)requestReplyMsg missing, result cached in replyCache
                    byte[] prevReply = replyCache.get(reqID).get(reqNum);
                    RequestReplyMessage rr =
                            new RequestReplyMessage(myID, reqNum, prevReply);
                    NodeIdentifier receiver =
                            new NodeIdentifier(NodeIdentifier.Role.CLIENT, reqID);
                    network.sendMessage(receiver, rr);
                    LOG.debug("send previous %s\n", rr);

                } else {
                    //2)decisionMsg missing, leave it for other part
                    // to request the missing decision
                    LOG.info("warning %s missing reply firstUn=%d firstHole=%d\n", msg, firstUnstable, firstHole);
                }
            }

        } else {
            synchronized (pendingDecisions) {
                if (pendingDecisions.get(index) != null) {
                    // stricter than (index < firstUnstable will also drop received pending decisions
                    decCount.addDrop();
                    LOG.debug("drop as already received decision %d, first_unstable %d\n",
                            index, firstUnstable);
                } else {// index >= firstUnstable

                    //case of msgIndex < firstUnstable make no sense
                    if (index < firstUnstable)
                        return;

                    maxReceived = Math.max(index, maxReceived);

                    //fill in slot[index]
                    pendingDecisions.put(index, msg);
                    decCount.addAccept();

                    // process learner sync snapshot
                    if (0 == (index-1) % catchupSSThreshold && !learnerSyncSSMsgBuf.isEmpty()) {
                        int sk = learnerSyncSSMsgBuf.firstKey();
                        LOG.info("process syncSS %d firstUn=%d index=%d\n", sk, firstUnstable, index);
                        processSyncSnapshot(learnerSyncSSMsgBuf.get(sk));
                        learnerSyncSSMsgBuf.remove(sk);
                    }

                    // process pending learner log in batch
                    if (lastPDCleanup > firstUnstable && index > lastPDCleanup && !inLoadSnapshot &&
                            catchupSnapshotDone && 0 == index % catchupLogThreshold) {
                        if (0 == (firstUnstable-1)%(maxPendingCatchup*syncChunkSize) &&
                                firstUnstable > lastLeaLogReadIndex) {
                            LOG.info("submit learnerLogRead(firstUn=%d) lastReadIdx=%d logIdx=%d hole=%d pdClean=%d\n",
                                    firstUnstable, lastLeaLogReadIndex, index, firstHole, lastPDCleanup);
                            //batchProcessLearnerLog(firstUnstable);
                            learnerLog.readLogChunk(firstUnstable);
                            lastLeaLogReadIndex = firstUnstable;
                        }
                    }

                    //NEED NOT add catchupSnapshotDone condition here because of firstUnstable guaranteed
                    if (index > firstUnstable) {
                        // cannot update state immediately, holes exist
                        if (!enableDecInParallel) {
                            leaderID = msg.getSender();
                        } else {
                            if (0 == index%20000)
                            LOG.debug("get index=%d > firstUn=%d firstHole=%d leaderID=%s\n", index,
                                firstUnstable, firstHole, leaderID);
                        }

                        //get a chance to update catchupDecisions from other learners
                        //printPattern(1, index);
                        updateMultiStates(firstUnstable, index);

                        // method 1: time consuming
                        // mark the index slot and used in updateStates()
                        // pad the empty slot with (key, null) mark

                        //for(long i=firstUnstable;i<index;i++) {
                        //    if (!pendingDecisions.containsKey(i)) {
                        //        pendingDecisions.put(i, null);
                        //        LOG.debug("LEA fill in empty slot[%d]\n", i);
                        //    }
                        //}

                        // method 2: only fill in slot[index], leave it to updateStates
                        LOG.debug("==> %s has holes, received: %d, firstUnstable: %d, maxReceived %d\n",
                                myID, index, firstUnstable, maxReceived);

                    } else { // index == firstUnstable
                        if (index == maxReceived) {
                            // index > previous largest received index, cannot equal
                            //printPattern(2, index);
                            updateState(msg);
                        } else if (index < maxReceived) { //todo skeptical position for learner catchup
                            //printPattern(3, index);
                            updateMultiStates(index, maxReceived);
                        }

                        //todo check %10000 => 1000
                        if (leaLogCatchupDone && !inTakeSnapshot && !inLoadSnapshot &&
                            0 == firstUnstable % 1000 && firstUnstable - checkPoint > ckPtInterval + maxPendingReq) {
                            long nextCkPt = (firstUnstable - maxPendingReq - 1) / ckPtInterval * ckPtInterval;
                            inTakeSnapshot = true;
                            appServerInterface.takeSnapshot(nextCkPt);
                        }
                    }

                    // update firstHole only when index == firstHole
                    if (index == firstHole) {
                        firstHole++;
                        // update in case following decisions arrive earlier
                        for (long k : pendingDecisions.tailMap(firstHole).keySet()) {
                            if (firstHole < k) {
                                break;
                            } else {
                                firstHole++;
                            }
                        }
                        if (firstHole - index > 5)
                            LOG.info("=>update firstHole=>%d, index.%d\n", firstHole, index);
                    }

                    //check pending decision holes
                    checkDecisionHoles(); //todo restrict catchupSnapshotDone & catchupDecisionDone

                    // use firstHole rather than pendingDecisions.size() or firstUnstable to control PD cap
                    if (firstHole - lastPDCleanup > maxPDCacheSize + maxPendingReq) {
                        long prevPD = lastPDCleanup;
                        lastPDCleanup += maxPDCacheSize;
                        long taa = System.nanoTime();
                        if (firstHole > firstUnstable) { //catchup case
                            for (long kk : pendingDecisions.subMap(prevPD + 1, lastPDCleanup + 1).keySet()) {
                                learnerLog.update(pendingDecisions.get(kk));
                            }
                        }
                        long tbb = System.nanoTime();
                        pendingDecisions.headMap(lastPDCleanup+1).clear();
                        LOG.info("pdCleanup<=%d, firstUn.%d, firstHole.%d, maxRec.%d, update=%.1f clear=%.1f ms\n",
                                lastPDCleanup, firstUnstable, firstHole, maxReceived,
                                (tbb-taa)/1000000.0, (System.nanoTime()-tbb)/1000000.0);
                    }
                }
            }
        }
    }

    private void checkCatchupDecisionHoles() {
        // (A) check learner decision catchup only after catchup/load snapshot done
        if (catchupSnapshotDone && !catchupDecisionDone) {
            if (null == leaderID) {
                locateLeader();
            } else {
                if (!catchupDecisionFinish) {
                    // have not receive leader SyncReply
                    if (0 == catchupDecisionEnd) {
                        // specific sync to request catchupDecisionEnd
                        network.sendMessage(leaderID, new SyncMessage(myID, 0, 0));
                    } else if (!catchupDecisionTrack.isEmpty()) {
                        Iterator<Map.Entry<Long, DecisionChunk>> iterator = catchupDecisionTrack.entrySet().iterator();
                        int tsDrift = 1000;
                        int count = 0;
                        while (iterator.hasNext() && count < maxPendingCatchup/2) {
                            Map.Entry<Long, DecisionChunk> entry = iterator.next();
                            if (System.currentTimeMillis() - entry.getValue().ts > batchOpTimeout) {
                                int mLen = entry.getValue().len;
                                catchupDecisionTrack.put(entry.getKey(), new DecisionChunk(mLen,
                                        System.currentTimeMillis()+tsDrift));
                                SyncMessage syncD = new SyncMessage(myID, entry.getKey(), mLen);
                                network.sendMessage(leaderID, syncD);
                                LOG.info("checkCatchupDecisionHoles send %s\n", syncD);
                                tsDrift += 1000;
                                count++;
                            }
                        }
                    }
                } else {
                    if (System.currentTimeMillis() - lastBatchOpTime > 500) { //avoid sending too much msg
                        lastBatchOpTime = System.currentTimeMillis();
                        //specific sync to indicate catchupDecision finishes
                        network.sendMessage(leaderID, new SyncMessage(myID, -1, 0));
                        LOG.info("not receive finishACK, resend catchupDecisionFinish\n");
                    }
                }
            }
        }
    }

    private void checkCatchupSnapshotHoles() {
        // (A) check learner snapshot catchup
        if (catchupLearner!=null && !catchupSnapshotDone) {
            if (!catchupSnapshotFinish) {
                if (!catchupSnapshotRecTrack.isEmpty()) {
                    Iterator<Map.Entry<Integer, Long>> iterator = catchupSnapshotRecTrack.entrySet().iterator();
                    int count = 0;
                    int tsDrift = 2000;
                    while (iterator.hasNext() && count < maxPendingCatchup/2) {
                        Map.Entry<Integer, Long> entry = iterator.next();
                        if (System.currentTimeMillis() - entry.getValue() > batchOpTimeout) {
                            catchupSnapshotRecTrack.put(entry.getKey(), System.currentTimeMillis()+tsDrift);
                            SyncSnapshotMessage syncSS = new SyncSnapshotMessage(myID, catchupSnapshotIdx,
                                    entry.getKey(), catchupSSExpected, 1);
                            network.sendMessage(catchupLearner, syncSS);
                            tsDrift += 2000;
                            count++;
                        }
                    }

                } else { //receive wakeup from leader, but have not received first sync reply
                    if (catchupSSStart > 0) { //skip in case catchupLearner has not receive 3.5R
                        //specific to request latest ckPt (0,1,0)
                        network.sendMessage(catchupLearner, new SyncSnapshotMessage(myID, catchupSnapshotIdx, 0, 1, 0));
                    }
                }
            } else {
                if (System.currentTimeMillis() - lastBatchOpTime > 100) { //avoid sending too much msg
                    lastBatchOpTime = System.currentTimeMillis();
                    network.sendMessage(catchupLearner, new SyncSnapshotMessage(myID, catchupSnapshotIdx, 0, 0, 0));
                    LOG.info("not receive snapshot finishACK, resend catchupSSFinish message\n");
                }
            }
        }
    }

    private void checkDecisionHoles() {
        long catchupIdx = maxReceived+1;
        long diff = catchupIdx - firstHole; //use firstHole instead of firstUnstable to track gap
        //LOG.debug("checkDecisionHoles firstHole.%d, maxRec.%d=>diff.%d\n", firstHole, maxReceived, diff);

        //decision might arrive out of order, wait a while (gap) before request missing decisions from leader
        if (diff > Configuration.maxDecisionGap) {
            // check pending catchup Decision sync
            if (!catchupDecisionTrack.isEmpty()) {
                int tsDrift = 400000; //2000;
                int count = 0;
                Iterator<Map.Entry<Long, DecisionChunk>> iterator = catchupDecisionTrack.entrySet().iterator();
                while (iterator.hasNext() && count < maxPendingCatchup/2) {
                    Map.Entry<Long, DecisionChunk> entry = iterator.next();
                    if (System.currentTimeMillis() - entry.getValue().ts > Configuration.acceptTimeout) {
                        catchupDecisionTrack.put(entry.getKey(), new DecisionChunk(entry.getValue().len,
                                System.currentTimeMillis()+tsDrift));
                        SyncMessage syncD = new SyncMessage(myID, entry.getKey(), entry.getValue().len);
                        network.sendMessage(leaderID, syncD);
                        LOG.info("checkDecisionHoles send %s\n", syncD);
                        tsDrift += 2000;
                        count++;
                    }
                }

            } else { //create new syncMsg
                long nextUnstable;
                synchronized (pendingDecisions) {
                    //restrict sync size
                    long endUnstable = Math.min(catchupIdx, firstHole + syncChunkSize);
                    for (nextUnstable = firstHole; nextUnstable < endUnstable; nextUnstable++) {
                        if (pendingDecisions.get(nextUnstable) != null)
                            break;
                    }

                    if (nextUnstable > firstHole) {
                        int len = (int) (nextUnstable - firstHole);
                        SyncMessage sync = new SyncMessage(myID, firstHole, len);
                        network.sendMessage(leaderID, sync);
                        catchupDecisionTrack.put(firstHole, new DecisionChunk(len, System.currentTimeMillis()));
                        LOG.info("checkDecisionHoles send %s firstHole %d pd(first %d, last %d)\n",
                                sync, firstHole, pendingDecisions.firstKey(), pendingDecisions.lastKey());
                    }
                }
            }
        }
    }

    private void processLearnerSleepDown(SleepDownMessage msg) {
        //pingAcceptorTracks.put(msg.getSender().hashCode(), System.currentTimeMillis());
        learnerIsActive = false;
        leaderID = msg.getSender(); //<= only leader will send sleepDown
        LOG.infoTS("receive %s isActive=%b leaderID=%s\n", msg, learnerIsActive, leaderID);
    }

    private void processLearnerWakeup(WakeupMessage msg) {

        catchupSSRecTime = System.nanoTime();

        //pingAcceptorTracks.put(msg.getSender().hashCode(), System.currentTimeMillis());
        learnerIsActive = true;
        long msgCkPt = msg.getCheckpoint();
        // disable init catchupDecisionEnd here, use 0 to block initAllCatchupDecisions
        //catchupDecisionEnd = msg.getMaxLogIndex();

        //catchupDecisionStart = Math.max(firstUnstable, msgCkPt+1);

        // CANNOT reset firstUnstable to skip catchup[start, end] BEFORE loadSnapshot
        //firstUnstable = catchupDecisionEnd+1;

        if (msgCkPt > checkPoint) {
            leaLogCatchupDone = false;
            catchupSnapshotFinish = false;
            catchupSnapshotDone = false;
            catchupSnapshotSendDone = false;
            //catchupSnapshotIdx = msgCkPt;
            catchupLearner = lastPingLearner;
            lastBatchOpTime = System.currentTimeMillis();
            if (catchupLearner != null) {
                //specific to request latest ckPt (0,1,0)
                network.sendMessage(catchupLearner, new SyncSnapshotMessage(myID, msgCkPt, 0, 1, 0)); //lc2s

            } else {
                LOG.error("error wakeup learner has no catchup learner\n");
                throw new RuntimeException("wakeup learner has no catchup learner\n");
            }
        } else {
            catchupSnapshotDone = true;
            catchupSnapshotSendDone = true;
            LOG.info("wakeup learner has up to date snapshot catchupSnapshotDone %b\n", catchupSnapshotDone);
        }

        LOG.debug("wakeup leaActive=%b ckPt=%d firstUn=%d catchupD[%d, %d] catchupSS=%d leaLogDone=%b\n",
                learnerIsActive, checkPoint, firstUnstable, catchupDecisionStart, catchupDecisionEnd,
                catchupSnapshotIdx, leaLogCatchupDone);
    }

    private void initAllCatchupDecisions() {
        if (leaderID == null) {
            LOG.error("error no leaderID\n");
            throw new RuntimeException("no leaderID for learner to catchup\n");
        }

        catchupDecisionSendDone = true;
        catchupPDRecTime = System.nanoTime();
        if (catchupDecisionEnd >= catchupDecisionStart) {
            long initStart = catchupDecisionStart;
            catchupDecisionFinish = false;
            catchupDecisionDone = false;
            int chunks = (int) (catchupDecisionEnd-catchupDecisionStart+syncChunkSize)/syncChunkSize;
            int sendCount = (chunks+maxPendingCatchup-1)/maxPendingCatchup;

            long cKey;
            int sendChunks, syncLen;
            int tsDrift = 40000; //40s
            SyncMessage sync;

            for (int i=0; i<sendCount; i++) {
                syncLen = maxPendingCatchup*syncChunkSize;
                sendChunks = maxPendingCatchup;

                tsDrift += 2000; //set timestamp drift for later sync messages due to delayed process on leader
                for (int s = 0; s < sendChunks; s++) {
                    cKey = catchupDecisionStart + syncChunkSize * s;
                    catchupDecisionTrack.put(cKey, new DecisionChunk(syncChunkSize,
                            System.currentTimeMillis()+tsDrift));
                }
                sync = new SyncMessage(myID, catchupDecisionStart, syncLen);

                catchupDecisionStart += maxPendingCatchup*syncChunkSize;
                network.sendMessage(leaderID, sync);
                //LOG.debug("catchupDecision %s =>%s\n", sync, leaderID);
            }
            LOG.info("=>catchupAllD start %d, end %d, chunks=%d #send=%d ckPt %d firstUn %d\n",
                    initStart, catchupDecisionEnd, chunks, sendCount, checkPoint, firstUnstable);

        } else {
            catchupDecisionFinish = true;
            catchupDecisionDone = true;
        }
    }

    private void initCatchupDecisions() {
        if (leaderID == null) {
            LOG.error("error no leaderID\n");
            throw new RuntimeException("no leaderID for learner to catchup\n");
        }

        catchupPDRecTime = System.nanoTime();
        if (catchupDecisionEnd >= catchupDecisionStart) {
            catchupDecisionFinish = false;
            catchupDecisionDone = false;
            catchupDecisionSendDone = false;
            int chunks = (int) (catchupDecisionEnd - catchupDecisionStart + syncChunkSize) / syncChunkSize;
            int sendChunks = Math.min(chunks, maxPendingCatchup);

            long cKey = 0;
            int tsDrift = 10000;
            for (int s = 0; s < sendChunks; s++) {
                cKey = catchupDecisionStart + syncChunkSize * s;
                catchupDecisionTrack.put(cKey, new DecisionChunk(syncChunkSize, System.currentTimeMillis()+tsDrift));
                tsDrift += 1000;
            }
            LOG.debug("wakeup catchupDecisionTrack add entry (%d, %d)\n", catchupDecisionStart, cKey);
            SyncMessage sync;
            if (chunks <= maxPendingCatchup) {
                catchupDecisionSendDone = true;
                sync = new SyncMessage(myID, catchupDecisionStart, (int) (catchupDecisionEnd - catchupDecisionStart + 1));
            } else {
                sync = new SyncMessage(myID, catchupDecisionStart, maxPendingCatchup * syncChunkSize);
                catchupDecisionStart += maxPendingCatchup * syncChunkSize;
                LOG.info("=>next[%d, %d] chunks %d ckPt %d firstUn %d catchupDone %b CatchupSendDone %b\n",
                        catchupDecisionStart, catchupDecisionEnd, chunks, checkPoint, firstUnstable,
                        catchupDecisionDone, catchupDecisionSendDone);
            }
            network.sendMessage(leaderID, sync);
            LOG.info("wakeup learner send %s ==> %s\n", sync, leaderID);
        } else {
            catchupDecisionFinish = true;
            catchupDecisionDone = true;
            catchupDecisionSendDone = true;
            LOG.info("wakeup learner has up to date decisions catchupDecisionDone %b\n", catchupDecisionDone);
        }
    }

    private void processPing(PingMessage msg) {
        NodeIdentifier sender = msg.getSender();
        long info = msg.getInfo();
        switch (sender.getRole()) {
            case ACCEPTOR:
                pingAcceptorTracks.put(sender.hashCode(), System.currentTimeMillis());
                if (info > 0 && (null == leaderID || leaderID.hashCode() != info)) {
                    leaderID = new NodeIdentifier((int) info);
                    LOG.infoTS("get new leader=%s <= %s\n", leaderID, msg);
                } else if (info == -4) {
                    //leader request FreeLogMessage again
                    FreeLogMessage freeLog = new FreeLogMessage(myID, checkPoint, 0);
                    for (int k : pingAcceptorTracks.keySet()) {
                        NodeIdentifier acceptor = new NodeIdentifier(k);
                        LOG.info("learner resend %s, firstUnstable %d\n", freeLog, firstUnstable);
                        network.sendMessage(acceptor, freeLog);
                    }
                } else if (info == -8) {
                    //leader request active learner
                    if (null == leaderID)
                        leaderID = sender;
                    PingMessage pingBack = new PingMessage(myID, (learnerIsActive?1:0));
                    network.sendMessage(sender, pingBack /*new PingMessage(myID, learnerIsActive?1:0)*/);
                    LOG.infoTS("reply %s => %s\n", learnerIsActive, pingBack, leaderID);
                }
                break;
            case LEARNER:
                pingLearnerTracks.put(sender.hashCode(), System.currentTimeMillis());

                //learner pingMsg contains local checkpoint
                lastPingLearner = msg.getSender();
                break;
            case CLIENT:
                break;
            default:
                LOG.debug("receive unexpected %s", msg);
                break;
        }
    }

    private void processLeaderSync(SyncMessage msg) {
        NodeIdentifier sender = msg.getSender();
        if (sender.getRole() != NodeIdentifier.Role.ACCEPTOR) {
            LOG.warning("receive wrong sync message from %s\n", sender);
        } else {
            if (-2 == msg.getStart()) { // set catchupSS timeCap slack ratio
                slackRatioSS = 0.9;
                LOG.info("get leader %s, slackRatioSS=%.1f\n", msg, slackRatioSS);

            } else if (-1 == msg.getLen() && -1 == msg.getStart()) { // LCatchup receive 3.5R
                // LCatchup 4 inform leader to start decision catchup
                {
                    network.sendMessage(leaderID, new SyncMessage(myID, 0, 0));

                    catchupSSStart = 1;
                    SyncSnapshotMessage syncSnapshot;
                    int tsDrift = 3000;
                    int num = maxPendingCatchup; // todo assume totalChunks >= maxPendingCatchup
                    for (int i = catchupSSStart; i < catchupSSStart + num; i++) {
                        catchupSnapshotRecTrack.put(i, System.currentTimeMillis()+tsDrift);
                        syncSnapshot = new SyncSnapshotMessage(myID, catchupSnapshotIdx, i, catchupSSExpected, 1);
                        network.sendMessage(catchupLearner, syncSnapshot);
                        tsDrift += 1000;
                    }
                    network.sendMessage(Configuration.clientIDs.get(1), new InfoMessage(myID, "LCatchup4"));
                    LOG.info("XXX LCatchup 4 syncSS(st %d, num %d)=>%s\n", catchupSSStart, num, catchupLearner);
                    catchupSSStart += num;
                }
            } else if (0 == msg.getLen() && 0 == catchupDecisionEnd) {
                //catchupDecisionEnd = msg.getStart();  //lc5r
                long chunkLen = syncChunkSize*maxPendingCatchup;
                catchupDecisionEnd = (msg.getStart()+chunkLen)/chunkLen * chunkLen; //round to next chunkLen
                firstHole = Math.max(firstHole, catchupDecisionEnd+1);

                LOG.info("lc5r catchupDEnd=%d hole=%d init decision catchup\n", catchupDecisionEnd, firstHole);
                //network.sendMessage(Configuration.clientIDs.get(1), new InfoMessage(myID, "lc5r"));

                // catchup decisions
                initAllCatchupDecisions();

                // catchup snapshot
                catchupSSStart = 1;
                SyncSnapshotMessage syncSnapshot;
                int tsDrift = 3000;
                int num = maxPendingCatchup; // assume totalChunks >= maxPendingCatchup
                for (int i = catchupSSStart; i < catchupSSStart + num; i++) {
                    catchupSnapshotRecTrack.put(i, System.currentTimeMillis()+tsDrift);
                    syncSnapshot = new SyncSnapshotMessage(myID, catchupSnapshotIdx, i, catchupSSExpected, 1);
                    network.sendMessage(catchupLearner, syncSnapshot);
                    tsDrift += 1000;
                }
                network.sendMessage(Configuration.clientIDs.get(1), new InfoMessage(myID, "lc6s"));
                LOG.info("lc6s syncSS st=%d num=%d =>%s\n", catchupSSStart, num, catchupLearner);
                catchupSSStart += num;
            }
        }
    }

    @Deprecated
    private void processSync(SyncMessage msg) {
        long start = msg.getStart();
        long end = start + msg.getLen();
        List<byte[]> decChunks = new ArrayList<>();
        int decLen = 0;
        synchronized (pendingDecisions) {
            for (long i = start; i < end; i++) {
                DecisionMessage decision = pendingDecisions.get(i);
                if (decision != null) {
                    decChunks.add(decision.toByteArray());
                    decLen += decision.toByteArray().length;
                    //network.sendMessage(receiver, decision);
                } else {
                    //leave it for checkDecisionHoles()
                    //SyncMessage sync = new SyncMessage(myID, i, 1);
                    //if (leaderID != null)
                    //    network.sendMessage(leaderID, sync);
                }
            }
        }
        int numDec = decChunks.size();
        byte[] chunk = new byte[decLen + 4 * numDec + 4];
        LOG.debug("create decisionChunk byteLen %d, #entries %d\n", chunk.length, numDec);

        ByteBuffer bb = ByteBuffer.wrap(chunk);
        bb.putInt(numDec);
        for (byte[] b : decChunks) {
            bb.putInt(b.length);
            bb.put(b);
        }
        network.sendMessage(msg.getSender(), new LearnerCatchupMessage(myID, start, numDec, chunk));
        LOG.info("send [%d,%d) #Dec %d learnerCatchup => %s\n", start, end, numDec, msg.getSender());
    }

    // sender side (1)
    private void receiveSyncSnapshot(SyncSnapshotMessage msg) {
        if (0 == msg.getNextChunk()) {
            if (0 == msg.getUnstable()) {
                catchupSnapshotSendCached.clear();
                catchupSendTokens.clear();
                // send specific SnapshotChunkMessage as finishACK
                network.sendMessage(msg.getSender(), new SnapshotChunkMessage(myID,
                        msg.getCheckpoint(), 0, -1, null, null));
                LOG.info("get catchupFinish reply ack, send(pure %.1f, all %.1f)(s)\n",
                        catchupAssembleSnapshotTime/1000.0, (System.nanoTime() - catchupSSSendTime)/1000000000.0);

            } else if (1 == msg.getUnstable()) {
                LOG.info("lc2r msgCKPt=%d ckPt=%d\n", msg.getCheckpoint(), checkPoint);

                //load snapshot tokens into catchupSnapshotSendCached
                if (0 == catchupSendTokens.size()) {
                    catchupSSSendTime = System.nanoTime();

                    ////catchupSendTokens = appServerInterface.getSnapshotTokens(ckPt);
                    catchupSendTokens = appServerInterface.getSnapshotTokens(checkPoint);
                    catchupSendTokensIterator = catchupSendTokens.listIterator();
                    tokenIteratorPos = 0;
                    if (0 == catchupSendTokens.size()) {
                        LOG.error("error snapshot.%d has no tokens\n", checkPoint);
                        return;
                    }
                    LOG.info("catchupSnapshotSendCached numChunks=%d of snapshot.%d time %.1f ms\n",
                            catchupSendTokens.size(), checkPoint, (System.nanoTime() - catchupSSSendTime) / 1000000.0);
                }

                network.sendMessage(msg.getSender(), new SnapshotChunkMessage(myID, checkPoint, 0,
                        catchupSendTokens.size(), null, null));    //lc3s
                //network.sendMessage(Configuration.clientIDs.get(1), new InfoMessage(myID, "lc3s"));
                LOG.info("lc3s ckPt=%d totalChunks=%d\n", checkPoint, catchupSendTokens.size());
            } else {
                LOG.error("error receive %s\n", msg);
            }
        } else {
            learnerSyncSSMsgBuf.put(msg.getNextChunk(), msg);
        }
    }

    // sender side (2)
    private void processSyncSnapshot(SyncSnapshotMessage msg) {

        int startChunk = msg.getNextChunk();
        long ckPt = msg.getCheckpoint();
        NodeIdentifier receiver = msg.getSender();

        if (!catchupSnapshotSendCached.isEmpty() && startChunk < catchupSnapshotSendCached.firstKey()) {
            LOG.info("already send snapshot chunk %d < firstKey %d\n", startChunk, catchupSnapshotSendCached.firstKey());
        } else {
            long startTotal = System.nanoTime();

            //batch load snapshot chunks and send SnapshotChunkMessage
            int totalChunks = catchupSendTokens.size();
            int endChunk = startChunk+msg.getNumChunks();
            byte[] token;
            byte[] ssChunk;
            if (startChunk <= tokenIteratorPos) {  //chunk should be cached in catchupSnapshotSendCached
                if (!catchupSnapshotSendCached.containsKey(startChunk)) {
                    LOG.error("error catchupSnapshotSendCached do not cache chunk.%d\n", startChunk);
                    throw new RuntimeException("catchupSnapshotSendCached do not cache chunk");
                } else {
                    LOG.info("=>resend missing chunk.%d, <=tokenIteratorPos %d\n", startChunk, tokenIteratorPos);
                    network.sendMessage(receiver, new SnapshotChunkMessage(myID, ckPt, startChunk, totalChunks,
                            catchupSnapshotSendCached.get(startChunk).token,
                            catchupSnapshotSendCached.get(startChunk).chunk));
                }
            } else {
                int i = startChunk;
                while (i < endChunk && catchupSendTokensIterator.hasNext()) {
                    token = catchupSendTokensIterator.next();
                    tokenIteratorPos++;
                    ssChunk = appServerInterface.getSnapshotChunk(token);
                    catchupSnapshotSendCached.put(i, new SnapshotCachedChunk(token, ssChunk));
                    network.sendMessage(receiver, new SnapshotChunkMessage(myID, ckPt, i, totalChunks, token, ssChunk));
                    //LOG.info("[M] replySnapshot chunk %d len %d => %s\n", i, ssChunk.length, receiver);
                    i++;
                }

                //cleanup to save memory cache usage
                int unstable = msg.getUnstable();
                if (!catchupSnapshotSendCached.isEmpty() &&
                        unstable - catchupSnapshotSendCached.firstKey() > 2*maxPendingCatchup) {
                    catchupSnapshotSendCached.headMap(unstable).clear();
                    LOG.info("catchupSnapshotSendCached cleanup<%d\n", unstable);
                }
            }

            double execTotal = (System.nanoTime() - startTotal)/1000000.0;
            catchupAssembleSnapshotTime += execTotal;
            LOG.info("fetch chunk[%d,%d]/%d %.1f ms, col %.1f s, all %.1f s\n",
                    startChunk, endChunk-1, totalChunks, execTotal, catchupAssembleSnapshotTime/1000.0,
                    (System.nanoTime()-catchupSSSendTime)/1000000000.0);

            if (isDynSSThresholdEnabled && startChunk>0.1*catchupSendTokens.size() && 0 == startChunk%10
                /*msg.getUnstable() > lastSSChunkUnstable*/) {
                lastSSChunkUnstable = msg.getUnstable();
                int granularity;
                if (Configuration.enableBatching) {
                    granularity = 10;
                } else {
                    granularity = 100;
                }

                if (useAvgMethod) {
                    int origSSThreshold = Configuration.catchupSnapshotThrottle;
                    double avgR = (System.nanoTime() - catchupSSSendTime) / (1000000.0 * lastSSChunkUnstable); //ms
                    // timeCap with slack ratio
                    double avgE = (Configuration.leaCatchupTimeCap*slackRatioSS) * 1000.0 / catchupSendTokens.size();
                    double tmpTh = 0;
                    if (avgR > avgE + 5 && catchupSSThreshold > 0.3*origSSThreshold) {
                        tmpTh = (double) catchupSSThreshold * Math.max(avgE / avgR, 0.7);
                    } else if (avgE > avgR + 5 && catchupSSThreshold < 2 * origSSThreshold) {
                        tmpTh = (double) catchupSSThreshold * Math.min(1.5, avgE / avgR);
                    }
                    if (tmpTh > 0.2*origSSThreshold) {
                        catchupSSThreshold = Math.round((int) tmpTh / granularity) * granularity;
                    }
                    LOG.info("lastSSChunkUn=%d, adjust catchupSSThreshold=%d, avgR=%.1f, avgE=%.1f tmpTh=%.1f\n",
                            lastSSChunkUnstable, catchupSSThreshold, avgR, avgE, tmpTh);
                } else {
                    double timeRatio = (Configuration.leaCatchupTimeCap-10)
                            / ((System.nanoTime() - catchupSSSendTime) / 1000000000.0);
                    double finRatio = (double) catchupSendTokens.size() / lastSSChunkUnstable;
                    if (finRatio > timeRatio + 0.01 && catchupSSThreshold > 4000) {
                        catchupSSThreshold -= 2000;
                    } else if (finRatio + 0.01 < timeRatio && catchupSSThreshold <
                            2 * Configuration.catchupSnapshotThrottle) {
                        catchupSSThreshold += 4000;
                    }
                    LOG.info("lastSSChunkUn=%d, adjust catchupSSThreshold=%d, finRatio=%.2f, timeRatio=%.2f\n",
                            lastSSChunkUnstable, catchupSSThreshold, finRatio, timeRatio);
                }
            }
        }

    }

    // receiver side (1)
    private void processCatchupSnapshotReply(SnapshotChunkMessage msg) {
        long ckPt = msg.getCheckpointIndex();
        int chunkOffset = msg.getChunkOffset();
        int totalChunks = msg.getTotalChunks();
        if (ckPt <= checkPoint || catchupSnapshotDone) {
            LOG.info("already receive/has snapshot.%d exit\n", ckPt);
        } else if (catchupSnapshotFinish) {
            if (0 == chunkOffset && -1 == totalChunks) {
                long loadStart = System.nanoTime();
                catchupSnapshotDone = true;

                catchupLearner = null;
                if (ckPt > checkPoint) {
                    //async loadSnapshot(): assume no additional learner failure during loadSnapshot()
                    inLoadSnapshot = true;
                    appServerInterface.loadSnapshot(ckPt); //async
                    //assert pdFirst == firstUnstable
                    LOG.info("=>finACK ckPt.%d catchEnd %d pd(Un.%d Ho.%d, pdClean.%d) loadSS(s) %.1f, recP=%.1f, A=%.1f\n",
                            checkPoint, catchupDecisionEnd, firstUnstable, firstHole, lastPDCleanup,
                            (System.nanoTime() - loadStart)/1000000000.0, catchupSnapshotTime/1000.0,
                            (System.nanoTime() - catchupSSRecTime)/1000000000.0);

                    network.sendMessage(Configuration.clientIDs.get(1), new InfoMessage(myID,
                            "catchupSnapshotFinish time(s)="+(System.nanoTime()-catchupSSRecTime)/1000000000));

                    // send new FreeLogMsg to all active acceptors
                    FreeLogMessage freeLog = new FreeLogMessage(myID, checkPoint, 0);
                    for (int k : pingAcceptorTracks.keySet()) {
                        NodeIdentifier acceptor = new NodeIdentifier(k);
                        network.sendMessage(acceptor, freeLog);
                        LOG.info("=>learner send %s=>%s\n", freeLog, acceptor);
                    }

                } else {
                    LOG.error("error sender maxReceived %d, catchupSSIdx %d <= checkPoint %d\n",
                            ckPt, catchupSnapshotIdx, checkPoint);
                }
            } else {
                network.sendMessage(msg.getSender(), new SyncSnapshotMessage(myID, catchupSnapshotIdx, 0, 0, 0));
                LOG.warning("warning finished but have not receive finishACK, resend it\n");
            }

        //receive first catchupSSReply
        } else if (0 == chunkOffset && totalChunks > 0) { //lc3r
            catchupSnapshotIdx = ckPt; //update if sender has more updated snapshot
            catchupDecisionStart = ckPt+1;
            catchupDecStartBackup = catchupDecisionStart;
            firstHole = Math.max(firstHole, catchupDecisionStart+1); //set firstHole
            lastPDCleanup = Math.max(lastPDCleanup, ckPt);
            catchupSSExpected = 1;

            network.sendMessage(leaderID, new SyncMessage(myID, catchupSnapshotIdx, -1)); //lc3.5s
            //network.sendMessage(Configuration.clientIDs.get(1), new InfoMessage(myID, "lc3.5s"));
            LOG.info("lc3.5s inform leader ssCkPt=%d 1stHole=%d lastPDCleanup=%d\n",
                    catchupSnapshotIdx, firstHole, lastPDCleanup);

        } else if (0 == totalChunks) {
            LOG.info("receive fake snapshot.%d chunk exit\n", ckPt);
        //} else if (chunkOffset < catchupSSExpected) {
        //    LOG.info("already processed chunk %d < expected %d\n", chunkOffset, catchupSSExpected);
        } else {
            long startTime = System.nanoTime();
            appServerInterface.storeSnapshotChunk(msg.getSnapshotToken(), msg.getSnapshotChunk());
            catchupSnapshotRecTrack.remove(chunkOffset);

            if (!catchupSnapshotRecTrack.isEmpty()) {
                catchupSSExpected = Math.max(catchupSSExpected, catchupSnapshotRecTrack.firstKey());
            }

            if (!catchupSnapshotSendDone && catchupSnapshotRecTrack.size() < maxPendingCatchup/2) {
                int chunks = totalChunks - catchupSSStart+1;
                if (chunks>0) {
                    int tsDrift = 1000;
                    int sendChunks = Math.min(chunks, maxPendingCatchup);
                    SyncSnapshotMessage syncSS;
                    for (int s = catchupSSStart; s < catchupSSStart+sendChunks; s++) {
                        ////catchupSnapshotReceiveCached.put(s, new SnapshotCachedChunk());
                        catchupSnapshotRecTrack.put(s, System.currentTimeMillis()+tsDrift);
                        syncSS = new SyncSnapshotMessage(myID, ckPt, s, catchupSSExpected, 1);
                        network.sendMessage(msg.getSender(), syncSS);
                        tsDrift += 1000; //sender side periodically fetch/reply snapshot chunk
                    }
                    LOG.info("send syncSS(st %d, num %d)=>%s\n", catchupSSStart, sendChunks, msg.getSender());

                    catchupSSStart += sendChunks;

                    if (chunks <= maxPendingCatchup) {
                        catchupSnapshotSendDone = true;
                    }
                    //LOG.info("next SSStart/total %d/%d\n", catchupSSStart, totalChunks);
                }
            }

            if (catchupSnapshotSendDone && catchupSnapshotRecTrack.isEmpty()) {
                catchupSnapshotFinish = true;
                network.sendMessage(msg.getSender(), new SyncSnapshotMessage(myID, catchupSnapshotIdx, 0, 0, 0));
                LOG.info("=>catchupSSFinish %b, Done %b, send Finish<>\n", catchupSnapshotFinish, catchupSnapshotDone);
            }

            lastBatchOpTime = System.currentTimeMillis();

            //(B) check receive snapshot catchup
            if (!catchupSnapshotRecTrack.isEmpty() && !catchupSnapshotFinish) {
                Iterator<Map.Entry<Integer, Long>> iterator = catchupSnapshotRecTrack.entrySet().iterator();
                //int chunkCount = 0;
                //int tsDrift = 2000;
                int tsDrift = 100000; //large 100s
                while (iterator.hasNext()) {
                    Map.Entry<Integer, Long> entry = iterator.next();
                    if (System.currentTimeMillis() - entry.getValue() > batchOpTimeout) {
                        catchupSnapshotRecTrack.put(entry.getKey(), System.currentTimeMillis()+tsDrift);
                        //tsDrift += 500;
                        //chunkCount++;
                        SyncSnapshotMessage syncSS = new SyncSnapshotMessage(myID, ckPt,
                                entry.getKey(), catchupSSExpected, 1);
                        network.sendMessage(msg.getSender(), syncSS);
                        break; //restrict only send one
                    }
                }
            }

            double execTime = (System.nanoTime() - startTime)/1000000.0;
            catchupSnapshotTime += execTime;
            LOG.info("process snapshot chunk.%d %.1f ms, col %.1f s, all %.0f s\n",
                    chunkOffset, execTime, catchupSnapshotTime/1000.0, (System.nanoTime()-catchupSSRecTime)/1000000000.0);
        }
    }

    private void receiveDecisionChunk(byte[] chunk, boolean isCatchup) {
        ByteBuffer bBuf = ByteBuffer.wrap(chunk);
        int numDec = bBuf.getInt();
        byte[] bString;
        DecisionMessage dec;
        if (isCatchup) { //catchup decision
            for (int i=0; i<numDec; i++) {
                bString = new byte[bBuf.getInt()];
                bBuf.get(bString);
                dec = new DecisionMessage();
                dec.fromByteArray(bString);
                catchupLeaLog.update(dec); //write to disk
            }
        } else { //new decision
            for (int i = 0; i < numDec; i++) {
                bString = new byte[bBuf.getInt()];
                bBuf.get(bString);
                dec = new DecisionMessage();
                dec.fromByteArray(bString);

                processDecision(dec);
            }
        }
    }

    private void processDecisionHoles(LearnerCatchupMessage msg) {
        long ck = msg.getFrom();
        if (ck < firstUnstable || msg.getNumOfDecisions() <= 0) {
            LOG.warning("warning =receive %s: either zero decisions or start<firstUn %d\n", msg, firstUnstable);
        } else if (!catchupDecisionTrack.containsKey(ck)) {
            LOG.warning("warning =catchup decision already processed entry.%d\n", ck);
        } else {
            receiveDecisionChunk(msg.getDecisionsBytes(), false);
            catchupDecisionTrack.remove(ck);
        }
        //need NOT check timeout due to processDecision() call checkDecisionHoles() frequently
    }

    private void processWholeLearnerLog() {
        long ts = System.nanoTime();
        leaLogIterator = learnerLog.iterator(-1);
        leaLogIteratorPos = 0;
        DecisionMessage decision;
        LOG.info("start process learnerLog decisions catchup(%d, %d), firstUn %d, pd(first %d, last %d)\n",
                catchupDecisionStart, catchupDecisionEnd, firstUnstable,
                pendingDecisions.firstKey(), pendingDecisions.lastKey());

        while (leaLogIterator.hasNext()) {
            decision = leaLogIterator.next();
            long index = decision.getIndex();
            //LOG.info("--> idx %d >= firstUn %d, pos %d\n", index, firstUnstable, leaLogIteratorPos);
            if (index >= firstUnstable) {
                processDecision(decision);
                leaLogIteratorPos++;
            }
        }
        //learnerLog.garbageCollect(leaLogIteratorPos-1);
        LOG.info("process learnerLog decisions (pos %d, firstUn %d) time %.1f s, pd(first %d, last %d)\n",
                leaLogIteratorPos, firstUnstable, (System.nanoTime() - ts) / 1000000000.0,
                pendingDecisions.firstKey(), pendingDecisions.lastKey());
    }

    @Override
    public void batchProcessLearnerLog(long start) {
        long tms = System.nanoTime();
        if (start <= catchupDecisionEnd) {
            if (start >= firstUnstable) {
                processCatchupLeaLog(start);
            } else {
                if (leaLogIterator != null) {
                    leaLogIterator = null;
                    LOG.warning("==>reset leaLogIterator=%b\n", leaLogIterator);
                }
                LOG.warning("batch process learnerLog [st=%d end=%d] start=%d < firstUn=%d\n",
                        catchupDecisionStart, catchupDecisionEnd, start, firstUnstable);
                return;
            }
        } else {
            processNewLeaLog(start);
        }
        double execTm = (System.nanoTime()-tms)/1000000.0;
        int granularity;
        int experiencedTime;
        if (Configuration.enableBatching) {
            granularity = 25;
            experiencedTime = 60; //60ms
        } else {
            granularity = 3000;
            experiencedTime = 100; //100ms
        }
        synchronized (pendingDecisions) {
            if (enableDynLogThreshold) {
                int tmpLogThreshold;
                if (execTm > experiencedTime) { //experienced value: 100ms for 20k decisions
                    int ratio = (int) Math.round(execTm / 100.0 * 10); //10x ratio
                    tmpLogThreshold = Math.min(syncChunkSize*(maxPendingCatchup-1), catchupLogThreshold*ratio/10);
                    tmpLogThreshold = Math.max(Configuration.catchupLeaLogThreshold*7/10, tmpLogThreshold);
                    LOG.info("execTm=%.1f ms, ratio=%d (10x)\n", execTm, ratio);
                } else {
                    tmpLogThreshold = Math.max(Configuration.catchupLeaLogThreshold*7/10,
                            catchupLogThreshold - 2*granularity);
                }
                catchupLogThreshold = tmpLogThreshold;
            }
            LOG.info("batch process learnerLog <st %d, itPos %d> %.0f ms firstUn=%d hole=%d catchupLogThreshold=%d\n",
                    start, leaLogIteratorPos, execTm, firstUnstable, firstHole, catchupLogThreshold);
        }
    }

    @Override
    public void syncLogComplete() {
        synchronized (pendingDecisions) {
        }
    }

    private void processNewLeaLog(long start) {
        //[ckPt+1, lastPDCleanup] multiple of (syncChunkSize * maxPendingCatchup)
        if (null == leaLogIterator || leaLogIteratorPos >= start) {
            leaLogIterator = learnerLog.iterator(start);
            LOG.info("reset leaLogIterator (learnerLog) pos %d >= start %d\n", leaLogIteratorPos, start);
        }
        leaLogIteratorPos = start-1; //firstUnstable-1
        DecisionMessage decision;
        long index;
        long end = start + syncChunkSize*maxPendingCatchup - 1;

        synchronized (pendingDecisions) {
            while (leaLogIterator.hasNext()) {
                decision = leaLogIterator.next();
                index = decision.getIndex();
                if (index >= start) {
                    leaLogIteratorPos++;
                    processDecision(decision);
                }
                if (index >= end) {
                    //leaLogIteratorPos = end;
                    if (0 == leaLogIteratorPos % ckPtInterval) {
                        learnerLog.garbageCollect(Math.min(firstUnstable - 1, leaLogIteratorPos));
                        LOG.info("learnerLog gc itPos=%d, firstUn=%d\n", leaLogIteratorPos, firstUnstable);
                    }
                    break;
                }
            }
        }

        if (leaLogIteratorPos == lastPDCleanup) {
            leaLogCatchupDone = true;
            learnerLog.garbageCollect(lastPDCleanup + 1);
            int bSz = Configuration.batchingSize;
            LOG.info("finish learnerLog pos %d == lastPDCleanup %d, LeaLogSz(M)=%.2f leaLogDone=%b\n", leaLogIteratorPos,
                    lastPDCleanup, (lastPDCleanup-catchupDecisionEnd+1)*bSz/1000000.0, leaLogCatchupDone);
            network.sendMessage(Configuration.clientIDs.get(1), new InfoMessage(myID,
                    "LearnerLogFinish len(M)=" + (lastPDCleanup-catchupDecisionEnd+1)*bSz/1000000.0));
            if (leaderID != null) {
                network.sendMessage(leaderID, new PingMessage(myID, -33)); //specific ping
                LOG.info("learner finish local log catchup\n");
            }
        }

        if (leaLogIteratorPos < end) {
            learnerLog.sync();
            learnerLog.readLogChunk(start); //re-add it
            LOG.info("=>sync learnerLog itPos %d < end %d\n", leaLogIteratorPos, end);
        }
    }

    private void processCatchupLeaLog(long start) {
        if (firstUnstable > start) {
            LOG.info("todo check firstUn=%d > start=%d\n", firstUnstable, start);
            return;
        }
        if (null == leaLogIterator || leaLogIteratorPos >= start) {
            leaLogIterator = catchupLeaLog.iterator(start);
            LOG.info("reset leaLogIterator (catchupLeaLog) pos %d >= start %d\n", leaLogIteratorPos, start);
        }
        leaLogIteratorPos = start-1; //firstUnstable-1
        DecisionMessage decision;
        long index;
        long end = start + syncChunkSize*maxPendingCatchup - 1;

        synchronized (pendingDecisions) {
            while (leaLogIterator.hasNext()) {
                decision = leaLogIterator.next();
                index = decision.getIndex();
                if (index >= start) {
                    leaLogIteratorPos++;
                    processDecision(decision);
                }
                if (index >= end) {
                    if (0 == leaLogIteratorPos % ckPtInterval) {
                        catchupLeaLog.garbageCollect(Math.min(firstUnstable-1, leaLogIteratorPos));
                        LOG.info("catchupLeaLog gc itPos=%d, firstUn=%d\n", leaLogIteratorPos, firstUnstable);
                    }
                    break;
                }
            }
        }

        if (leaLogIteratorPos == catchupDecisionEnd) {
            catchupLeaLog.garbageCollect(catchupDecisionEnd+1);
            leaLogIterator = null;
            int bSz = Configuration.batchingSize;
            LOG.info("finish catchupLeaLog pos %d == catchupDEnd %d, catchupLeaLogSz=%.2f M\n",
                    leaLogIteratorPos, catchupDecisionEnd, (catchupDecisionEnd-catchupDecStartBackup+1)*bSz/1000000.0);
            network.sendMessage(Configuration.clientIDs.get(1), new InfoMessage(myID,
                    "catchupLearnerLogFinish len(M)=" + (catchupDecisionEnd-catchupDecStartBackup+1)*bSz/1000000.0));
        }

        if (leaLogIteratorPos < end) {
            catchupLeaLog.sync();
            catchupLeaLog.readLogChunk(start); //re-add it
            LOG.info("=>sync catchupLeaLog start=%d itPos %d < end %d\n", start, leaLogIteratorPos, end);
        }
    }

    private void processDecisionCatchup(LearnerCatchupMessage msg) {
        long ck = msg.getFrom();
        if (catchupDecisionDone) {
            LOG.info("catchup decision done %b\n", catchupDecisionDone);
        } else if (catchupDecisionFinish) {
            if (0 == ck) {
                catchupDecisionDone = true;
                LOG.info("catchup decision done=>%b\n", catchupDecisionDone);
            } else { //resend specific syncFin
                LOG.error("error catchupDecision reply %s\n", msg);
                network.sendMessage(msg.getSender(), new SyncMessage(myID, -1, 0));
            }
        } else if (!catchupDecisionTrack.containsKey(ck)) {
            LOG.warning("warning catchup decision already processed entry.%d\n", ck);
        } else {
            long start = System.nanoTime();
            //todo: be aware of "No-op" decisions
            if (msg.getNumOfDecisions() <= 0) {
                LOG.warning("warning receive 0 decisions %s\n", msg);
                return;
            }

            if (ck <= firstUnstable) {
                receiveDecisionChunk(msg.getDecisionsBytes(), false);
                LOG.info("receiveDecisionChunk ck=%d <= firstUn=%d\n", ck, firstUnstable);
            } else {
                receiveDecisionChunk(msg.getDecisionsBytes(), true);
            }
            catchupDecisionTrack.remove(ck);

            //todo: check modified condition, use headmap to filter new missing decisions
            if (catchupDecisionSendDone && catchupDecisionTrack.headMap(catchupDecisionEnd+1).isEmpty()) {
            //if (catchupDecisionSendDone && catchupDecisionTrack.isEmpty()) {
                catchupDecisionFinish = true;
                catchupLeaLog.sync();

                LOG.info("<=>catchup decision finish %b firstUn.%d, hole.%d, rec(pure %.1f, all %.1f)(s)\n",
                        catchupDecisionFinish, firstUnstable, firstHole, catchupPDecisionTime / 1000.0,
                        (System.nanoTime() - catchupPDRecTime) / 1000000000.0);

                if (null == leaderID) {
                    locateLeader();
                } else {
                    //todo: consider remove this round, not necessary
                    network.sendMessage(msg.getSender(), new SyncMessage(myID, -1, 0));  //LCatchup(7A)
                }
                network.sendMessage(Configuration.clientIDs.get(1), new InfoMessage(myID,
                        "catchupDecisionFinish time(s)="+(System.nanoTime()-catchupPDRecTime)/1000000000));

                // method sequentially catchup decisions after snapshot and process new decisions from learnerLog
                //processWholeLearnerLog();
            }

            //(B) check learner catchup
            if (!catchupDecisionTrack.isEmpty() && !catchupDecisionFinish) {
                Iterator<Map.Entry<Long, DecisionChunk>> iterator = catchupDecisionTrack.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<Long, DecisionChunk> entry = iterator.next();
                    if (System.currentTimeMillis() - entry.getValue().ts > batchOpTimeout) {
                        catchupDecisionTrack.put(entry.getKey(), new DecisionChunk(entry.getValue().len,
                                System.currentTimeMillis()));
                        SyncMessage syncD = new SyncMessage(myID, entry.getKey(), entry.getValue().len);
                        network.sendMessage(msg.getSender(), syncD);
                        LOG.info("B catchupD resend %s\n", syncD);
                        break;  //break restricts send just one sync
                    }
                }
            }

            lastBatchOpTime = System.currentTimeMillis();

            double execTime = (System.nanoTime() - start)/1000000.0;
            catchupPDecisionTime += execTime;
            LOG.info("catchupPDRec=%d firstUn=%d %.1f ms, col %.1f s, all %.0f s\n", ck, firstUnstable, execTime,
                    catchupPDecisionTime/1000.0, (System.nanoTime()-catchupPDRecTime)/1000000000.0);
        }
    }

	@Override
	public void handleMessage(Message msg) {
        switch (Message.MSG_TYPE.values()[msg.getType()]) {
            case DECISION:
                processDecision((DecisionMessage) msg);
                break;
            case PING:
                processPing((PingMessage) msg);
                break;
            case SYNC:
                //processSync((SyncMessage) msg);
                processLeaderSync((SyncMessage) msg);
                break;
            case SYNCSNAPSHOT:
                receiveSyncSnapshot((SyncSnapshotMessage) msg);
                //processSyncSnapshot((SyncSnapshotMessage) msg);
                break;
            case SNAPSHOTCHUNK:
                processCatchupSnapshotReply((SnapshotChunkMessage) msg);
                break;
            case LCATCHUP:
                LearnerCatchupMessage lCatchup = (LearnerCatchupMessage) msg;
                if (lCatchup.getFrom() > catchupDecisionEnd) { //todo: make sure condition is correct
                //if (catchupDecisionDone && firstUnstable > catchupDecisionEnd) {
                    processDecisionHoles((LearnerCatchupMessage) msg);
                } else {
                    processDecisionCatchup((LearnerCatchupMessage) msg);
                }
                break;
            case WAKEUP:
                processLearnerWakeup((WakeupMessage) msg);
                break;
            case SLEEPDOWN:
                processLearnerSleepDown((SleepDownMessage) msg);
                break;
            default:
                LOG.error("error learner received unsupported message\n");
                throw new RuntimeException("Learner received unsupported message\n");
        }
	}

	@Override
	public void handleTimer() {
        //check catchup snapshot snapshot holes
        checkCatchupSnapshotHoles();
        //check catchup decision holes
        checkCatchupDecisionHoles();

        checkDecisionHoles();
	}

    private void getMemoryStat() {
        LOG.info("MEM_INFO catchup(decTrack %d, SSSendCached %d, SSReceiveCached %d), pDecision %d, firstUnstable %d\n",
            catchupDecisionTrack.size(), catchupSnapshotSendCached.size(),
            catchupSnapshotRecTrack.size(), pendingDecisions.size(), firstUnstable);
    }

    private class PingTimer extends TimerTask {
        @Override
        public void run() {
            decCountCycle++;
            if (0 == decCountCycle%10) { //assume pingInterval 200ms
                //showReqCountInfo();
                //getMemoryStat();
            }

            //send ping messages
            for (int k : pingLearnerTracks.keySet()) {
                //skip myself
                if (k == myID.hashCode())
                    continue;

                //possibly the learner is down
                if (pingLearnerTracks.get(k) != 0 &&
                    System.currentTimeMillis() - pingLearnerTracks.get(k) > Configuration.learnerTimeout) {
                    //pingLearnerTracks.remove(k);
                    //LOG.warning("leaTimeout remove %s, remain %s\n", new NodeIdentifier(k), pingLearnerTracks.keySet());
                    pingLearnerTracks.put(k, System.currentTimeMillis());
                    LOG.warning("=>%s timeout reset\n", new NodeIdentifier(k));
                }
                else {
                    //network.sendMessage(new NodeIdentifier(k), new PingMessage(myID, firstUnstable));
                    network.sendMessage(new NodeIdentifier(k), new PingMessage(myID, checkPoint));
                }
            }

            for (int p : pingAcceptorTracks.keySet()) {
                if (pingAcceptorTracks.get(p) != 0 &&
                        System.currentTimeMillis() - pingAcceptorTracks.get(p) > Configuration.acceptTimeout) {
                    //pingAcceptorTracks.remove(p);
                    //LOG.warning("#accTimeout remove %s, remain %s, leaderID %s\n",
                    //        new NodeIdentifier(p), pingAcceptorTracks.keySet(), leaderID);
                    pingAcceptorTracks.put(p, System.currentTimeMillis()+400);
                    LOG.warning("=>%s timeout reset\n", new NodeIdentifier(p));
                }
                else {
                    // learner should inform (new) leader its active/inactive status
                    PingMessage ping = new PingMessage(myID, (learnerIsActive?1:0));
                    network.sendMessage(new NodeIdentifier(p), ping /*new PingMessage(myID, learnerIsActive?1:0)*/);
                    //LOG.info("send %s => %s\n", ping, new NodeIdentifier(p));
                }
            }
        }
    }

    public static void main(String []args) throws Exception {
        int rank = Integer.parseInt(args[0]);
        String configFile = args[1];
        Configuration.initConfiguration(configFile);
        Learner learner = new Learner();
        AppServerInterface app = new HashTableServer(Configuration.learnerSnapshotDir);
        learner.setApplicationServer(app);
        learner.start(rank, configFile);

        NodeIdentifier lastLearner = new NodeIdentifier(NodeIdentifier.Role.LEARNER, 3);
        long numDecisions = 150;
        long testCheckpoint = 100;

        if (learner.myID.equals(lastLearner)) {
            //learner.LOG.debug("call testLearnerFetchSnapshot\n");
            //learner.testLearnerFetchSnapshot(numDecisions, testCheckpoint, app);
        } else {
            LOG.debug("call testLearnerCreateSnapshot\n");
            learner.testLearnerCreateSnapshot(numDecisions, testCheckpoint, app);
        }
    }

    private void testLearnerCreateSnapshot(long numDecisions, long ck, AppServerInterface app) {
        NodeIdentifier fakeClient = Configuration.clientIDs.get(1);
        ClientRequest cr;
        String str = "hello world from ";
        for(long i=1; i<=numDecisions; i++) {
            byte[] val = (str + String.valueOf(i)).getBytes();
            byte[] req = new byte[1 + 8 + val.length];
            ByteBuffer b = ByteBuffer.wrap(req);
            b.put((byte)1);
            b.putLong(i);
            b.put(val);
            cr = new ClientRequest(fakeClient.getID(), i, req);
            byte[] chunk = new byte[req.length + 4 + 4];
            ByteBuffer bb = ByteBuffer.wrap(chunk);
            bb.putInt(1); // numReq
            bb.putInt(req.length);
            bb.put(req);
            DecisionMessage decision = new DecisionMessage(myID, i, chunk);
            pendingDecisions.put(i, decision);

            //updateState(decision);
            Request wReq = new Request();
            // wReq.request = decision.getRequest();
            // wReq.client = new NodeIdentifier(NodeIdentifier.Role.CLIENT, decision.getReqID());
            // wReq.clientSeqID = decision.getReqNum();
            wReq.request = req;
            wReq.client = new NodeIdentifier(NodeIdentifier.Role.CLIENT, fakeClient.getID());
            wReq.clientSeqID = i;
            appServerInterface.executeRequest(wReq);
            //LOG.debug("put <%d, %s>\n", i, decision);

            if (i == ck) {
                takeSnapshotComplete(ck); //<= will cleanup pendingDecisions
                //app.takeSnapshot(ck);
                checkPoint = ck;
            }
        }
        firstUnstable = numDecisions + 1;
        maxReceived = numDecisions;
    }

    private void testLearnerFetchSnapshot(long numDecisions, long ck, AppServerInterface app) throws InterruptedException {
        checkDecisionHoles();
        SyncMessage sync = new SyncMessage(myID, firstUnstable, (int) numDecisions);
        for(int k : Configuration.learnerIDs.keySet()) {
            NodeIdentifier dst = Configuration.learnerIDs.get(k);
            if (!dst.equals(myID)) {
                network.sendMessage(dst, sync);
            }
        }
        int prev = pendingDecisions.size();
        Thread.sleep(5000);
        app.loadSnapshot(ck);
        if (firstUnstable < numDecisions) {
            sync = new SyncMessage(myID, firstUnstable, (int) numDecisions);
            for(int k : Configuration.learnerIDs.keySet()) {
                NodeIdentifier dst = Configuration.learnerIDs.get(k);
                if (!dst.equals(myID)) {
                    network.sendMessage(dst, sync);
                }
            }
        }
        Thread.sleep(5000);
        int pos = pendingDecisions.size();
        System.out.format("pendingDecisions prev %d, pos %d\n", prev, pos);
        if (pos > 0) {
            for (long j : pendingDecisions.keySet()) {
                System.out.format("pd %d, %s\n", j, pendingDecisions.get(j));
            }
        }
    }

	@Override
	public void handleFailure(NodeIdentifier node, Throwable cause) {
        if (!Configuration.enableFailDetection) {
            LOG.debug("LFailure %s:%s\n", node, cause);
            return;
        }

        if (cause instanceof ClosedChannelException) {
            LOG.warning(" => catch %s ClosedChannelException\n", node);
            int nodeHash = node.hashCode();
            if (pingLearnerTracks.containsKey(nodeHash)) {
                pingLearnerTracks.remove(nodeHash);
                LOG.info("remove %s from pingLearnerTracks\n", node);

                if (!catchupSnapshotDone && catchupLearner.equals(node)) {
                    throw new RuntimeException(node + " failure during catchup procedure");
                }

            } else if (pingAcceptorTracks.containsKey(nodeHash)) {
                pingAcceptorTracks.remove(nodeHash);
                LOG.info("remove %s from pingAcceptorTracks\n", node);
                if (leaderID!=null && leaderID.equals(node)) {
                    leaderID = null;
                    //locateLeader(); //todo check whether needed
                }
            } else {
                LOG.debug("neither pingLearnerTracks or pingAcceptorTracks contains %s\n", node);
            }
        } else {
            LOG.debug(" ==>%s:%s\n", node, cause);
        }
	}

    private void printPattern(int symbol, long count) {
        if (catchupSnapshotDone) {
            if (0 == count % syncChunkSize) {
                System.out.printf("p\n");
            } else {
                System.out.printf("%d", symbol);
            }
        }
    }
}
