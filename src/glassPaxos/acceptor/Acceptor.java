package glassPaxos.acceptor;

import glassPaxos.Configuration;
import glassPaxos.SimpleLogger;
import glassPaxos.interfaces.AcceptorLogCbInterface;
import glassPaxos.network.EventHandler;
import glassPaxos.network.NettyNetwork;
import glassPaxos.network.Network;
import glassPaxos.network.NodeIdentifier;
import glassPaxos.network.messages.*;
import glassPaxos.storage.AcceptorLog;
import glassPaxos.utils.MessageProfiler;
import glassPaxos.utils.RequestCountProfiler;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class Acceptor implements EventHandler, AcceptorLogCbInterface {
	private NodeIdentifier myID = null;
	private Network network = null;
    private NodeIdentifier leaderID = null;
    private boolean isLeader = false;       //for all acceptors
    private boolean leaderSelected = false; //for leader only
    private boolean isActive = true;        //for other acceptors

    private int maxRound = 0;
    private int electionRound = 0;

    private long maxLogIndex = 0;
    private long firstUnstableIndex = 1;
    private long currentIndex = 0;
    private long checkpointIndex = 0;

    //leader catchup
    private long catchupStart = 0;
    private long catchupEnd = 0;
    private boolean catchupSendDone = false;
    private boolean catchupDone = false;
    private final HashMap<Long, Quorum> leaderCatchupQuorums = new HashMap<Long, Quorum>();

    private long ltoTimestamp = 0;  //0.normal; -1.started; ts.timestamp
    private boolean reProposeDone = false;

    //follower catchup
    private long acceptorCatchupStart = 0;
    private long acceptorCatchupEnd = 0;
    private boolean acceptorCatchupSendDone = false;
    private boolean acceptorCatchupDone = true;
    private final TreeMap<Long, AcceptMsgChunk> acceptorCatchupTrack = new TreeMap<Long, AcceptMsgChunk>();
    private final TreeMap<Long, SyncMessage> acceptSyncMsgBuf = new TreeMap<Long, SyncMessage>();
    private boolean switchLearnerDone = true;
    private boolean learnerCatchupLogDone = true;
    private boolean switchAcceptorDone = true;
    private long batchAcceptedStartIndex;

    private double slackRatioAcc = 0.95;

    private final HashMap<Integer, TreeMap<Long, RequestMessage>> futureRequests =
            new HashMap<Integer, TreeMap<Long, RequestMessage>>();
    private final TreeMap<Long, AcceptMessage> acceptorLogCache = new TreeMap<Long, AcceptMessage>();
    private long lastLogCacheCleanIndex;
    private long reProposeLastCacheIndex;
    private long reProposeLastCacheCleanIndex;

    private Quorum prepareQuorum;
    private final HashMap<Long, Quorum> acceptQuorums = new HashMap<Long, Quorum>();
    private final Map<Integer, Integer> acceptSet = new LinkedHashMap<Integer, Integer>();
    private final Map<Integer, Integer> backupSet = new LinkedHashMap<Integer, Integer>();

    private final Map<Integer, Integer> learnerActiveSet = new LinkedHashMap<Integer, Integer>();
    private final Map<Integer, Integer> learnerInActiveSet = new LinkedHashMap<Integer, Integer>();
    private final HashSet<Integer> blockedSwapInLearners  = new HashSet<Integer>();
    private final HashSet<Integer> blockedSwapInAcceptors = new HashSet<Integer>();

    private final TreeMap<Long, HashSet<Integer>> freeAcceptorLogQuorum =
            new TreeMap<Long, HashSet<Integer>>();

    private final ConcurrentHashMap<Integer, Long> pingAcceptorTracks = new ConcurrentHashMap<Integer, Long>();
    private final ConcurrentHashMap<Integer, Long> pingLearnerTracks = new ConcurrentHashMap<Integer, Long>();

    // keep track of requestID: <clientID, maxReceived>, <clientHash, maxAccepted>
    private final HashMap<Integer, Long> maxRequestID = new HashMap<Integer, Long>();

    private static SimpleLogger LOG;
    private AcceptorLog acceptorLog;
    private AcceptorLog catchupAccLog;

    private Iterator<AcceptMessage> accLogIterator = null;
    private Iterator<AcceptMessage> accDecIterator = null;
    private long accLogIteratorPos = 0;
    private long accDecIteratorPos = 0;

    private long tsSwitchFollowerStart;
    private long tsSwitchNewLeaderStart;
    private int leaCatchupDThreshold;
    private int accCatchupThreshold;
    private final TreeMap<Long, SyncMessage> learnerSyncMsgBuf = new TreeMap<Long, SyncMessage>();
    private long catchupFetchDecisionTime;  // total time for learner catchup decision chunks
    private double catchupFDTotalTime;      // accumulated pure fetch time for learner catchup decision chunks
    private long countCycle = 0L;
    private long leaCatchupDStart;          //guarded by execution order
    private long leaCatchupDLen;            //guarded


    // // single-threaded fastPath
    // private LinkedBlockingQueue<AcceptMessage> fAccQueue = new LinkedBlockingQueue<AcceptMessage>(1000);
    // private LinkedBlockingQueue<AcceptedMessage> fAccdQueue = new LinkedBlockingQueue<AcceptedMessage>(1000);

    // profiling
    private double tsPrefetch;
    private double tsMerge;
    private double tsReProposeR;
    private double tsReProposeS;
    private double tsReProposeUp;
    private double tsProfileA;
    private double tsProfileB;
    private final RequestCountProfiler reqCount = new RequestCountProfiler();
    private final MessageProfiler msgProfiler = new MessageProfiler();

    // system config parameter
    private static int maxPendingCatchup;
    private static int syncAccChunkSize;
    private static int syncChunkSize;
    private static int ckPtInterval;
    // constant
    private static final boolean isDynThresholdEnabled = true;
    private static final boolean useAvgMethod = true;
    //private static final boolean isMultithreading = false;
    private static final int SHOW_INTERVAL = 300000;

    private static int maxAccLogCacheSize;      //default 400k
    private static boolean enableBatching;      //batching mode default off
    private static boolean enableDecInParallel; //parallel decision mode default off
    private static boolean enableFastPath;      //leader fastPath put accept/accepted into event queue directly

    // batching
    private long lastBatchingTime;
    private long pendingReqCount;
    private static int BATCH_SIZE;
    //private static final int BATCH_SIZE = 50;       //50 requests
    private static final long BATCH_TIMEOUT = 100;  //100ms

    //multi-thread
    // private ProcessRequestThread pRequestThread = new ProcessRequestThread();
    // private ProcessAcceptThread pAcceptThread = new ProcessAcceptThread();
    // private ProcessAcceptedThread pAcceptedThread = new ProcessAcceptedThread();


	public Acceptor(NodeIdentifier myID){
		this.myID = myID;
		this.network = new NettyNetwork(myID, this);
        // pRequestThread.start();
        // pAcceptThread.start();
        // pAcceptedThread.start();
	}

    @Override
    public void backgroundTask(long index) {
        // todo send to ALL if leaderID=null
        long tsSyncStart = System.currentTimeMillis();
        //acceptorLog.sync();
        catchupAccLog.moveDirectory(Configuration.catchupAccLogDir, Configuration.acceptorLogDir);

        NodeIdentifier lid;
        lid = leaderID;
        network.sendMessage(lid, new PingMessage(myID, -16));  // inform leader catchupDecision done
        LOG.info(">>bgTask index=%d acceptorCatchupDone=%b firstUn=%d MOVEb=%.1f(s)\n", index, acceptorCatchupDone,
                firstUnstableIndex, (System.currentTimeMillis()-tsSyncStart)/1000.0);
    }

    private class AcceptMsgChunk {
        public AcceptMsgChunk(int len, long ts) {
            this.len = len;
            this.ts = ts;
        }
        int len;
        long ts;
    }

	public static void main(String []args) throws Exception {
        int rank = Integer.parseInt(args[0]);
        String configFile = args[1];
        Configuration.initConfiguration(configFile);
        Configuration.showNodeConfig();
		Acceptor acceptor = new Acceptor(Configuration.acceptorIDs.get(rank));
        Configuration.displayMemoryInfo(acceptor.myID.toString());

    	Configuration.addActiveLogger("NettyNetwork", SimpleLogger.INFO);
    	Configuration.addActiveLogger("Acceptor", Configuration.debugLevel);

        LOG = SimpleLogger.getLogger("Acceptor");
        maxPendingCatchup = Configuration.maxPendingCatchupSync;
        syncChunkSize = Configuration.maxSyncDiff;
        syncAccChunkSize = Configuration.maxAccSyncDiff;
        ckPtInterval = Configuration.checkpointInterval;
        acceptor.initParameter();

        acceptor.informClient();
        acceptor.startTimer();
        if (acceptor.leaderID == null) {
            acceptor.startLeaderElection();
        } else {
            LOG.debug("already get leaderID %s, isLeader %b\n", acceptor.leaderID, acceptor.isLeader);
        }
    }

    private void informClient() {
        for (int c : Configuration.clientIDs.keySet()) {
            NodeIdentifier cid = Configuration.clientIDs.get(c);
            network.sendMessage(cid, new PingMessage(myID, 0));
            LOG.info("informClient send ping =>%s\n", cid);
        }
    }

    private void startTimer() {
        Timer timer = new Timer();
        long pingInterval = Configuration.pingTimeout/4;
        // timer delay should be set to 0, or else frequent change for leadership
        timer.scheduleAtFixedRate(new PingTimer(), 0, pingInterval);
    }

    private void initParameter() {
        acceptorLog   = new AcceptorLog(Configuration.acceptorLogDir, this);
        catchupAccLog = new AcceptorLog(Configuration.catchupAccLogDir, this);

        prepareQuorum = new Quorum(Configuration.numAcceptors, 0);

        for (int i : Configuration.clientIDs.keySet()) {
            TreeMap cMap = new TreeMap<>();
            NodeIdentifier cID = Configuration.clientIDs.get(i);
            futureRequests.put(cID.hashCode(), cMap);
            maxRequestID.put(cID.hashCode(), 0L); //init maxReceived
            maxRequestID.put(i, 0L);  //init maxAccepted
            LOG.debug("create futureRequest for %s\n", cID);
        }
        //lastAccLogCacheCleanIndex = 0L;

        for (int hKey : Configuration.acceptorIDs.keySet()) {
            int hHash = Configuration.acceptorIDs.get(hKey).hashCode();
            prepareQuorum.qrmap.put(hHash, new QuorumEntry(false, 0L));
            //pingAcceptorTracks.put(hHash, 0L);
            pingAcceptorTracks.put(hHash, System.currentTimeMillis());
        }

        for (int lk : Configuration.learnerIDs.keySet()) {
            //pingLearnerTracks.put(Configuration.learnerIDs.get(lk).hashCode(), 0L);
            pingLearnerTracks.put(Configuration.learnerIDs.get(lk).hashCode(), System.currentTimeMillis());
        }

        catchupFDTotalTime = 0L;
        leaCatchupDThreshold = Configuration.catchupDecisionThrottle;
        accCatchupThreshold  = Configuration.catchupDecisionThrottle;

        msgProfiler.initMsgTiming();
        // msgProfiler.initFuncTiming();

        LOG.info("init pingAcceptorTracks %s, pingLearnerTracks %s, leaCatchupDThreshold=%d\n",
                pingAcceptorTracks.keySet(), pingLearnerTracks.keySet(), leaCatchupDThreshold);

        BATCH_SIZE     = Configuration.batchingSize;
        enableBatching = Configuration.enableBatching;
        maxAccLogCacheSize = Configuration.maxLogCacheSize;
        enableDecInParallel = Configuration.enableDecInParallel;
        enableFastPath = Configuration.enableLeaderFastPath;
        LOG.info("enableBatching=%b enableDecInParallel=%b enableFastPath=%b maxLogCacheSize=%d\n",
                enableBatching, enableDecInParallel, enableFastPath, maxAccLogCacheSize);
        lastBatchingTime = System.currentTimeMillis();
    }

    private void showReqCountInfo() {
        LOG.info(reqCount.displayCounterChange());
        reqCount.updateLastCounters();
    }

    private void showClientReqInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("maxRequestID ");
        for(Map.Entry<Integer, Long> entry : maxRequestID.entrySet()) {
            sb.append("<").append(entry.getKey()).append(",").append(entry.getValue()).append("> ");
        }
        sb.append("\n");
        LOG.debug(sb.toString());
    }

    private void modifyAcceptQuorumsEntry(long qr_key, boolean mode) {
        if (mode) {  //true: creation
            Quorum accept_quorums_entry = new Quorum(Configuration.numQuorumAcceptors, 0);
            for (int i : acceptSet.keySet()) {
                int hk = (new NodeIdentifier(i)).hashCode();
                accept_quorums_entry.qrmap.put(hk, new QuorumEntry(false, System.currentTimeMillis()));
            }
            acceptQuorums.put(qr_key, accept_quorums_entry);
        } else { //false: deletion
            acceptQuorums.remove(qr_key);
        }
        //LOG.debug("acceptQuorums sz %d, contains[%d]? %b\n", acceptQuorums.size(),
        //        qr_key, acceptQuorums.containsKey(qr_key));
    }

    private void startLeaderElection() {
        electionRound = Math.max(electionRound, maxRound);
        electionRound++;
        LOG.debug("leader electionRound %d, maxRound %d\n", electionRound, maxRound);

        try {
            Thread.sleep(20*myID.getID()); //sleep proportionally to ID rank
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if ((electionRound - myID.getID()) % Configuration.numAcceptors == 0) {
            tsSwitchNewLeaderStart = System.currentTimeMillis();
            isLeader = true;
            leaderSelected = false;
            catchupSendDone = false;
            catchupDone = false;
            reProposeDone = false;
            acceptSet.clear();
            backupSet.clear();
            prepareQuorum.resetQuorum();

            PrepareMessage prepare;
            maxRound = electionRound;
            prepare = new PrepareMessage(myID, maxRound, checkpointIndex, firstUnstableIndex);
            LOG.info("leader election maxRound=%d, electionRound=%d, isLeader=%b, leaderSelected=%b\n",
                    maxRound, electionRound, isLeader, leaderSelected);

            network.sendMessage(Configuration.clientIDs.get(1), new InfoMessage(myID, "start switch leader election"));

            for (NodeIdentifier acc : Configuration.acceptorIDs.values()) {
                prepareQuorum.qrmap.put(acc.hashCode(),
                        new QuorumEntry(false, System.currentTimeMillis()));
                network.sendMessage(acc, prepare);
            }
            LOG.info("send prepare => %s\n", Configuration.acceptorIDs.values());

        } else {
            //it's not my turn, but send fake prepare to get leaderID/maxRound updated
            for (int j : Configuration.acceptorIDs.keySet()) {
                NodeIdentifier dst = Configuration.acceptorIDs.get(j);
                if (!dst.equals(myID)) { //skip send to myself
                    network.sendMessage(dst, new PrepareMessage(myID, 0, checkpointIndex, firstUnstableIndex));
                    LOG.info("maxRound %d, electionRound %d, send fake prepare to %s\n",
                            maxRound, electionRound, dst);
                }
            }
        }
    }

    private void processPing(PingMessage msg) {
        NodeIdentifier nodeID = msg.getSender();
        switch (nodeID.getRole()) {
            case ACCEPTOR:
                int sender = nodeID.hashCode();
                pingAcceptorTracks.put(sender, System.currentTimeMillis());
                if (leaderSelected && isLeader) {
                    if (!acceptSet.containsKey(sender) && !backupSet.containsKey(sender)) {
                        backupSet.put(sender, 0);
                        network.sendMessage(nodeID, new SleepDownMessage(myID)); //todo optimize vdr
                        LOG.info("leader add %s =>backupSet=%s acceptSet=%s set %s sleep\n",
                                nodeID, backupSet.keySet(), acceptSet.keySet(), nodeID);
                    }
                    if (msg.getInfo() == -16) { //follower finish catchup accept
                        switchAcceptorDone = true; //set flag to true
                        //ltoTimestamp = System.currentTimeMillis();
                        ltoTimestamp = 0;
                        LOG.info("=>set switchAcceptorDone=%b\n", switchAcceptorDone);
                    }
                } else {
                    //general acceptors need not know catchupFinished
                    if (msg.getInfo() == -1 && reProposeDone) {
                        reProposeDone = false;
                        LOG.info("set reProposeDone=%b <= %s\n", reProposeDone, msg);
                    } else if (msg.getInfo() == 0 && !reProposeDone) {
                        reProposeDone = true;
                        LOG.info("set reProposeDone Y=%b <= %s\n", reProposeDone, msg);
                    }
                }

                break;

            case CLIENT:
                //cut down overhead and pingTracks will not store clientID
                //pingTracks.put(nodeID.hashCode(), System.currentTimeMillis());

                //if (leaderSelected && isLeader) //<== only leader will reply round
                if (leaderID == null) {  //<== every acceptor will reply round
                    return;
                }

                PingMessage pc = new PingMessage(myID, maxRound);
                network.sendMessage(msg.getSender(), pc);
                LOG.debug("send leader info %s => %s\n", pc, nodeID);
                break;

            case LEARNER:
                int leaHash = nodeID.hashCode();
                pingLearnerTracks.put(leaHash, System.currentTimeMillis());
                if (msg.getInfo() == -5) {
                    if (leaderSelected && leaderID != null) {
                        network.sendMessage(nodeID, new PingMessage(myID, leaderID.hashCode()));
                        LOG.info("reply (-5) leaderID=%s\n", leaderID);
                    }
                } else if (msg.getInfo() == -33) { //learner finish local catchup
                    learnerCatchupLogDone = true; //set flag
                    LOG.info("leader set learnerCatchupLogDone=%b\n", learnerCatchupLogDone);

                } else if (!isLeader || !leaderSelected) { //follower
                    //blank
                } else { //proposer
                    if (!blockedSwapInLearners.contains(leaHash)) {
                        if (!learnerActiveSet.containsKey(leaHash) && !learnerInActiveSet.containsKey(leaHash)) {
                            LOG.debug("receive %s active=%s, inactive=%s\n", msg, learnerActiveSet.keySet(),
                                learnerInActiveSet.keySet());

                            if (1 == msg.getInfo()) {

                                if (learnerActiveSet.keySet().size() < Configuration.numQuorumLearners) {
                                    learnerActiveSet.put(leaHash, 0);
                                    LOG.info("add %d=>learner activeSet=%s inActiveSet=%s\n", leaHash,
                                            learnerActiveSet.keySet(), learnerInActiveSet.keySet());

                                } else {
                                    NodeIdentifier pairAcc =
                                            new NodeIdentifier(NodeIdentifier.Role.ACCEPTOR, nodeID.getID());
                                    if (Configuration.protocolType == Configuration.PROTOCOL_TYPE.THRIFTY &&
                                            !learnerActiveSet.containsKey(leaHash) &&
                                            (myID.equals(pairAcc) || acceptSet.containsKey(pairAcc.hashCode()))) {
                                        Iterator<Integer> it = learnerActiveSet.keySet().iterator();
                                        int backupLea = 0;
                                        while (it.hasNext()) {
                                            backupLea = it.next();
                                            int backupID = (new NodeIdentifier(backupLea)).getID();
                                            pairAcc = new NodeIdentifier(NodeIdentifier.Role.ACCEPTOR, backupID);
                                            if (backupSet.containsKey(pairAcc.hashCode())) {
                                                // if (!myID.equals(pairAcc) && !acceptSet.containsKey(pairAcc.hashCode())) {
                                                LOG.info("break myID=%s backup pairAcc=%s acceptSet=%s\n", myID, pairAcc,
                                                        acceptSet.keySet());
                                                break;
                                            } else {
                                                if (!it.hasNext()) {
                                                    backupLea = 0;
                                                    LOG.debug("warning no suitable victim learner for %s acceptSet=%s\n",
                                                            nodeID, acceptSet.keySet());
                                                }
                                            }
                                        }

                                        if (backupLea == 0) { //todo check
                                            // learnerInActiveSet.put(leaHash, -1);
                                            // network.sendMessage(nodeID, new SleepDownMessage(myID));
                                            LOG.info("warning backupLea=%d <= %s\n", backupLea, nodeID);
                                            locateLearner();
                                        } else if (backupLea != leaHash) {
                                            learnerActiveSet.remove(backupLea);
                                            learnerActiveSet.put(leaHash, 0);
                                            learnerInActiveSet.put(backupLea, -1);
                                            network.sendMessage(new NodeIdentifier(backupLea), new SleepDownMessage(myID));
                                            LOG.info("THRIFTY switch leaHash=%d set %s sleep\n", leaHash,
                                                    new NodeIdentifier(backupLea));
                                        } else {
                                            LOG.warning("wrong backup learner=%d == leaHash=%d\n", backupLea, leaHash);
                                        }
                                    } else {
                                        learnerInActiveSet.put(leaHash, -1);
                                        network.sendMessage(nodeID, new SleepDownMessage(myID));
                                        LOG.info("general set %s sleep\n", nodeID);
                                    }
                                    LOG.info("info=%d add %d=>learner inActiveSet=%s activeSet=%s\n", msg.getInfo(),
                                            leaHash, learnerInActiveSet.keySet(), learnerActiveSet.keySet());

                                }
                            } else {
                                learnerInActiveSet.put(leaHash, -1);
                                network.sendMessage(nodeID, new SleepDownMessage(myID));
                                LOG.info("msgInfo=%d set %s sleep\n", msg.getInfo(), nodeID);

                            }
                            //  //network.sendMessage(nodeID, new PingMessage(myID, leaderID.hashCode()));
                            //  locateLearner();
                        }
                    }
                }
                //network.sendMessage(nodeID, new PingMessage(myID, 0L));
                break;
            default: //no inter-client pings
                LOG.debug("receive unexpected ping message from %s", nodeID.toString());
                break;
        }
    }

    private int getNextElectionRound(int round) {
        int k = round;
        while ((k-myID.getID())%Configuration.numAcceptors != 0) {
            k++;
        }
        return k;
    }

    private void processPrepare(PrepareMessage msg) {
        pingAcceptorTracks.put(msg.getSender().hashCode(), System.currentTimeMillis());

        int msgRound = msg.getRound();

        if (msgRound > maxRound && msg.getFirstUnstable() < firstUnstableIndex) {
            //falling behind acceptor init leader election

            electionRound = Math.max(electionRound, msgRound); //todo CHECK (change from maxRound -> msgRound)
            electionRound++;
            electionRound = getNextElectionRound(electionRound); //todo CHECK (change from maxRound -> electionRound)

            maxRound = electionRound;
            PrepareMessage rePrepare = new PrepareMessage(myID, maxRound, checkpointIndex, firstUnstableIndex);
            isLeader = true;
            leaderSelected = false;
            catchupSendDone = false;
            catchupDone = false;
            reProposeDone = false;
            acceptSet.clear();
            backupSet.clear();
            prepareQuorum.resetQuorum();
            LOG.info("retry leader election maxRound=%d, electionRound=%d, isLeader=%b, leaderSelected=%b <=msgRound=%d\n",
                    maxRound, electionRound, isLeader, leaderSelected, msgRound);

            tsSwitchNewLeaderStart = System.currentTimeMillis();
            for (NodeIdentifier acc : Configuration.acceptorIDs.values()) {
                prepareQuorum.qrmap.put(acc.hashCode(), new QuorumEntry(false, System.currentTimeMillis()));
                network.sendMessage(acc, rePrepare);
            }
            LOG.info("retry LE send %s => %s\n", rePrepare, Configuration.acceptorIDs.values());
            return; //todo check whether needs to skip reply PromiseMessage

        } else if (msgRound >= maxRound && msgRound > 0) {
            maxRound = msgRound;

            // firstUnstableIndex = msg.getIndex(); //todo reset firstUnstable to solve fall behind new leader catchup
            firstUnstableIndex = msg.getCheckpoint()+1;
            if (! myID.equals(msg.getSender()))
                isLeader = false;
            leaderID = msg.getSender();
            LOG.info("msgRound=%d leaderID=%s <= %s\n", msgRound, leaderID, msg);
            isActive = true;

        } else { // fake Prepare
            int accHash = msg.getSender().hashCode();
            if (isLeader && leaderSelected && !acceptSet.containsKey(accHash)) {
                if (!backupSet.containsKey(accHash)) {
                    backupSet.put(accHash, 0);
                    network.sendMessage(msg.getSender(), new SleepDownMessage(myID));
                    LOG.info("get fake prepare, I am leader put %s to sleep\n", msg.getSender());
                } else {
                    LOG.warning("warning get fake prepare backupSet=%s has acc=%d\n", backupSet.keySet(), accHash);
                }
            }
        }

        LOG.info("%s receive %s prepare leaderID=%s, maxRound=%d, isLeader=%b, isActive=%b, Unstable=>%d\n",
                myID, msg.getSender(), leaderID, maxRound, isLeader, isActive, firstUnstableIndex);
        PromiseMessage promise = new PromiseMessage(myID, maxRound, currentIndex, maxLogIndex);
        network.sendMessage(msg.getSender(), promise);
    }

    private boolean leaderSwitchLearner(int swapLearner) {
        if (!blockedSwapInLearners.isEmpty() ||
            (swapLearner!=0 && !learnerActiveSet.containsKey(swapLearner)) ||
            learnerInActiveSet.isEmpty()) {
            LOG.info("abandon switch %s blockedSwapIn=%s ActiveSet=%s inActiveSet=%s\n",
                    new NodeIdentifier(swapLearner), Arrays.toString(blockedSwapInLearners.toArray()),
                    learnerActiveSet.keySet(), learnerInActiveSet.keySet());
            return false;

        } else {
            int swapIn = learnerInActiveSet.keySet().iterator().next();
            learnerInActiveSet.remove(swapIn);
            // Should not add learnerInActiveSet, leave it for processPing to add
            //learnerInActiveSet.add(swapLearner);

            learnerActiveSet.remove(swapLearner);
            blockedSwapInLearners.add(swapIn);
            //delay adding backup learner into activeSet until learner nearly catchup leader
            //learnerActiveSet.put(swapIn, System.currentTimeMillis());

            learnerCatchupLogDone = false;

            // send SleepDown and Wakeup Msg
            if (swapLearner != 0) {
                network.sendMessage(new NodeIdentifier(swapLearner), new SleepDownMessage(myID));
                LOG.info("send SleepDown=>%d\n", swapLearner);
            }
            leaCatchupDStart = checkpointIndex + 1;

            FreeLogMessage freeLog = new FreeLogMessage(myID, checkpointIndex, swapLearner);
            for (int k : acceptSet.keySet()) {
                network.sendMessage(new NodeIdentifier(k), freeLog);
                LOG.info("=>leader send %s =>%s\n", freeLog, new NodeIdentifier(k));
            }

            if (!Configuration.enableFailDetection) ltoTimestamp = System.currentTimeMillis();

            network.sendMessage(Configuration.clientIDs.get(1), new InfoMessage(myID, "LCatchup1"));
            LOG.info("LCatchup 1, swapOut=%s blockedLearners %s, firstUn %d, learnerActiveSet %s\n",
                    new NodeIdentifier(swapLearner), Arrays.toString(blockedSwapInLearners.toArray()),
                    firstUnstableIndex, learnerActiveSet.keySet());
            return true;
        }
    }

    private void leaderSwitchAcceptor(long startQuorumIndex, int swapOut) {

        if (acceptSet.isEmpty()) {
            throw new RuntimeException("acceptSet is empty");

        } else if (!acceptSet.containsKey(swapOut) || backupSet.isEmpty()) {
            if (acceptQuorums.get(startQuorumIndex)!=null) {
                acceptQuorums.get(startQuorumIndex).qrmap.put(swapOut, new QuorumEntry(false, System.currentTimeMillis()));
            }
            LOG.info("swapOut=%d not in acceptSet=%s or backupSet=%s exit\n", swapOut,
                    acceptSet.keySet(), backupSet.keySet());
            return;
        }

        Quorum qr;
        int swapIn;
        // swap between backupSet and acceptSet
        swapIn = backupSet.keySet().iterator().next();
        backupSet.remove(swapIn);
        acceptSet.remove(swapOut);
        acceptSet.put(swapIn, 0);

        // NOT add swapOut acceptor into backupSet if it fails
        LOG.info("acceptSet=%s backupSet=%s LActive=%s LinActive=%s\n",
                acceptSet.keySet(), backupSet.keySet(),
                learnerActiveSet.keySet(), learnerInActiveSet.keySet());

        NodeIdentifier swapAcceptor = new NodeIdentifier(swapOut);
        network.sendMessage(swapAcceptor, new SleepDownMessage(myID));
        LOG.info("switchAcc send SleepDown=>%s\n", swapAcceptor);

        NodeIdentifier acc = new NodeIdentifier(swapIn);

        //if currentIndex > maxLogIndex, leader might have items in acceptQuorums but not acceptLogCache
        //long end = Math.max(maxLogIndex, currentIndex);
        //todo let client timeout to request [maxLogIndex, currentIndex]

        if (maxLogIndex < currentIndex && enableFastPath) {
            LOG.info("wrong todo enableFastPath=%b maxLog=%d < cur=%d\n",
                    enableFastPath, maxLogIndex, currentIndex);
            /*try {
                while (!fAccQueue.isEmpty()) {
                    AcceptMessage pAccept = fAccQueue.take();
                    LOG.info("switch take %s and process\n", pAccept);
                    processAccept(pAccept, 0);
                }
                while (!fAccdQueue.isEmpty()) {
                    AcceptedMessage pAccepted = fAccdQueue.take();
                    LOG.info("switch take %s and process\n", pAccepted);
                    leaderProcessAccepted(pAccepted);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/
        }

        long end = Math.max(maxLogIndex, currentIndex);
        //(1) rePropose [firstUnstable, maxLogIndex]
        if (startQuorumIndex <= end) {
            AcceptMessage reAccept;
            int tsDrift = 1000;
            for (long i = startQuorumIndex; i <= end; i++) {
                if (!acceptQuorums.containsKey(i)) {
                    //String exceptionMessage = "acceptQuorums does not contain index " + startQuorumIndex;
                    //throw new RuntimeException(exceptionMessage);
                    LOG.warning("todo acceptQuorums does not contain index %d, firstUnstable %d\n",
                            startQuorumIndex, firstUnstableIndex); //todo check vdr bug here
                } else {
                    qr = acceptQuorums.get(i);
                    qr.qrmap.put(swapIn, new QuorumEntry(false, System.currentTimeMillis()+tsDrift));
                    qr.qrmap.remove(swapOut);
                    if (i > maxLogIndex) { //range [maxLog, cur]
                        LOG.warning("todo mt bug i=%d maxLog=%d cur=%d accLogCache.containsKey(i)=%b\n",
                                i, maxLogIndex, currentIndex, acceptorLogCache.containsKey(i));
                    }
                    if (!acceptorLogCache.containsKey(i)) {
                        LOG.warning("todo acceptorLogCache does not contain %d\n", i);
                    } else {
                        reAccept = acceptorLogCache.get(i);
                        network.sendMessage(new NodeIdentifier(swapIn), reAccept);
                        LOG.debug("resend %d: %s ==> %s\n", i, reAccept, new NodeIdentifier(swapIn));
                    }
                }
                if (i == end)
                    LOG.info("#switch follower rePropose(%d, %d, len=%d)=>%s, firstUn=%d maxLog=%d curIdx=%d\n",
                            startQuorumIndex, end, (end-startQuorumIndex+1), acc, firstUnstableIndex,
                            maxLogIndex, currentIndex);

                tsDrift += 1000;
            }
            network.sendMessage(Configuration.clientIDs.get(1), new InfoMessage(myID,
                    "pre-switch follower reProposeLen=" + (end - startQuorumIndex)));
        }

        switchAcceptorDone = false;
        acceptorCatchupStart = checkpointIndex+1;
        acceptorCatchupEnd = startQuorumIndex-1;
        blockedSwapInAcceptors.add(swapIn);

        //(2) wakeupSync [checkpoint+1, firstUnstable-1]
        network.sendMessage(acc, new WakeupMessage(myID, checkpointIndex, startQuorumIndex-1));
        LOG.info("wakeup follower %s ckPt=%d maxLogIndex=%d currentIndex=%d blockedAcc=%s\n", acc, checkpointIndex,
                maxLogIndex, currentIndex, Arrays.toString(blockedSwapInAcceptors.toArray()));
    }

    private void processPendingLearnerSync() {
        long tStart = System.nanoTime();
        long sk = learnerSyncMsgBuf.firstKey();

        interactiveProcessLearnerSync(learnerSyncMsgBuf.get(sk));
        learnerSyncMsgBuf.remove(sk);
        catchupFDTotalTime += (System.nanoTime()-tStart)/1000000.0;
        int eachChunk = maxPendingCatchup * syncChunkSize;
        int granularity;
        if (enableBatching) {
            granularity = 10;
        } else {
            granularity = 1000;
        }

        if (isDynThresholdEnabled && sk-leaCatchupDStart>leaCatchupDLen*0.1 && 1 == sk%eachChunk) {
            if (useAvgMethod) {
                int origDThreshold = Configuration.catchupDecisionThrottle;
                double avgR = ((System.nanoTime() - catchupFetchDecisionTime)/1000000.0)
                        * eachChunk/(sk-leaCatchupDStart);
                double avgE = Configuration.leaCatchupTimeCap * slackRatioAcc * 1000.0
                        * eachChunk/leaCatchupDLen; // timeCap with slackRatioAcc
                double tmpTh = 0;
                if (avgR > avgE + 2 && leaCatchupDThreshold > 0.3 *origDThreshold) {
                    tmpTh = (double) leaCatchupDThreshold * Math.max(avgE / avgR, 0.7);
                } else if (avgE > avgR + 2 && leaCatchupDThreshold < 3 * origDThreshold) {
                    tmpTh = (double) leaCatchupDThreshold * Math.min(1.5, avgE / avgR);
                }
                if (tmpTh > 0.2*origDThreshold) {
                    leaCatchupDThreshold = Math.round((int) tmpTh / granularity) * granularity;
                }
                LOG.info("sk=%d adjust leaCatchupDThreshold=%d avgR=%.3f, avgE=%.3f tmpTh=%.1f\n",
                        sk, leaCatchupDThreshold, avgR, avgE, tmpTh);
            } else {
                double timeRatio = (Configuration.leaCatchupTimeCap-20)
                        / ((System.nanoTime() - catchupFetchDecisionTime) / 1000000000.0);
                double finRatio = 0.0;
                if (sk > leaCatchupDStart) {
                    finRatio = (double) leaCatchupDLen / (sk - leaCatchupDStart);
                }
                if (finRatio > timeRatio + 0.01 && leaCatchupDThreshold > 4000) {
                    leaCatchupDThreshold -= 2000;
                } else if (finRatio + 0.01 < timeRatio && leaCatchupDThreshold
                        < 2 * Configuration.catchupDecisionThrottle) {
                    leaCatchupDThreshold += 4000;
                }
                LOG.info("sk=%d adjust leaCatchupDThreshold=%d finR=%.2f, tmR=%.2f\n",
                        sk, leaCatchupDThreshold, finRatio, timeRatio);
            }
        }

        LOG.info("process pending learner catchup st.%d firstUn %d lastClean %d eachChunk=%d\n",
                sk, firstUnstableIndex, lastLogCacheCleanIndex, eachChunk);
    }

    private void locateLearner() {
        for (NodeIdentifier lea : Configuration.learnerIDs.values()) {
            network.sendMessage(lea, new PingMessage(myID, -8));
        }
        LOG.debug("locate Learner ping(-8)=>%s\n", Configuration.learnerIDs.values());
    }

    private void leaderProcessPromise(PromiseMessage msg) {
        int msgRound = msg.getRound();
        NodeIdentifier sender = msg.getSender();
        int senderHash = sender.hashCode();

        pingAcceptorTracks.put(senderHash, System.currentTimeMillis());

        if (msgRound == 0) { //todo check
            LOG.warning("fake prepare reply msgRound=%d maxRound=%d return\n", msgRound, maxRound);
            return;
        }

        if (msgRound != maxRound) {
            if (msgRound > maxRound) { //preempted
                int rank = msgRound % Configuration.numAcceptors;
                if (0 == rank)
                    rank = Configuration.numAcceptors;
                leaderID = new NodeIdentifier(NodeIdentifier.Role.ACCEPTOR, rank);
                isLeader = false;
                maxRound = msgRound;
                //acceptSet.clear();
                //backupSet.clear();
                LOG.info("preempt by %s, update maxRound=%d\n", leaderID, msgRound);
            }
            return;
        } else {
            if ((maxRound - myID.getID()) % Configuration.numAcceptors != 0) {
                int rank = msgRound % Configuration.numAcceptors;
                if (0 == rank)
                    rank = Configuration.numAcceptors;
                leaderID = new NodeIdentifier(NodeIdentifier.Role.ACCEPTOR, rank);
                LOG.info("I am not the leader, FAKE leaderID=%s <= %s\n", leaderID, msg);
                return;
            }
        }

        QuorumEntry qe = new QuorumEntry(true, 0); //clear QuorumEntry
        if (!prepareQuorum.qrmap.get(senderHash).getDone()) {
            prepareQuorum.incQuorum();
            prepareQuorum.qrmap.put(senderHash, qe);
            LOG.info("#prepareQuorum put %d quorum=%d\n", senderHash, prepareQuorum.getQuorum());
        }

        if (!leaderSelected) {
            acceptSet.put(senderHash, 0); // add hashcode of NodeID into acceptSet
            LOG.info("add %s=>acceptSet=%s\n", msg.getSender(), acceptSet.keySet());

            catchupEnd = Math.max(catchupEnd, msg.getMaxLogIndex());
            if (catchupEnd > Configuration.checkpointInterval) {
                //todo: might be wrong here
                long subtractPendingReq = catchupEnd - Configuration.maxPendingRequests * Configuration.numClients;
                checkpointIndex = Math.max(subtractPendingReq / Configuration.checkpointInterval *
                        Configuration.checkpointInterval, checkpointIndex);
                //firstUnstableIndex = Math.max(firstUnstableIndex, checkpointIndex+1);
            }

            //todo add more info in PromiseMsg, combine catchupStart, firstUn
            firstUnstableIndex = checkpointIndex + 1;  //vdr move outside of if()
            catchupStart = checkpointIndex + 1;
            reProposeLastCacheIndex = firstUnstableIndex;
            reProposeLastCacheCleanIndex = firstUnstableIndex;
            LOG.info("new leader merge catchup(%d, %d) reset ckPt=%d FirstUn=%d <=%s\n",
                    catchupStart, catchupEnd, checkpointIndex, firstUnstableIndex, msg.getSender());

        } else {
            //in case leader receive itself Promise later than other acceptors
            //switch one acceptor into backupSet
            int backup;
            if (senderHash == myID.hashCode() && !acceptSet.containsKey(senderHash)) {
                backup = acceptSet.keySet().iterator().next();
                if (backup != senderHash) {
                    acceptSet.remove(backup);
                    acceptSet.put(senderHash, 0);
                    backupSet.put(backup, 0);
                    LOG.info("replace %s=>backupSet=%s %s=>acceptSet=%s leaderSelected=%b\n",
                        new NodeIdentifier(backup), backupSet.keySet(), myID, acceptSet.keySet(), leaderSelected);
                } else {
                    backup = -1;
                    LOG.warning("get WRONG itself Promise acceptSet=%s backupSet=%s\n", acceptSet.keySet(),
                            backupSet.keySet());
                }
            } else {
                if (!acceptSet.containsKey(senderHash)) {
                    backup = senderHash;
                    backupSet.put(backup, 0);
                    LOG.info("add %s => backupSet=%s acceptSet=%s leaderSel=%b\n", msg.getSender(),
                            backupSet.keySet(), acceptSet.keySet(), leaderSelected);
                } else {
                    backup = -1;
                    LOG.warning("get duplicate Promise sHash=%d acceptSet=%s backupSet=%s\n",
                            senderHash, acceptSet.keySet(), backupSet.keySet());
                }
            }
            if (backup > 0) {
                network.sendMessage(new NodeIdentifier(backup), new SleepDownMessage(myID));
                LOG.info("leader send SleepDown=>%s\n", new NodeIdentifier(backup));
            }
        }

            /* reach consensus with f+1 out of 2f+1 */
        if (prepareQuorum.recvQuorum() && !leaderSelected) {
            leaderSelected = true;
            isLeader = true;
            locateLearner(); //make sure activeLearner is recorded in learnerActiveSet

            tsPrefetch = tsMerge = tsReProposeS = tsReProposeR = tsReProposeUp = 0.0;
            tsProfileA = tsProfileB = 0.0;
            LOG.info("reach consensus with %d acceptors, total %d\n",
                    prepareQuorum.getQuorum(), prepareQuorum.getTotal());
        }

        //new leader catch up acceptLogCache catchupQuorums (K: startIndex, V: Quorum)
        //quorum number of Quorum = #acceptors - 1
        if (leaderSelected && acceptSet.containsKey(myID.hashCode())) {
            if (catchupStart <= catchupEnd) {
                catchupDone = false;
            } else {
                catchupDone = true;
                reProposeDone = true;
                return;
            }

            if (firstUnstableIndex <= catchupEnd) {
                reProposeDone = false;
            } else {
                catchupDone = true; //todo verify this
                reProposeDone = true;
                return;
            }

            LOG.info("new leader catchup(%d, %d, len=%d) cur=%d catchupDone=%b reProposeDone=%b <=%s\n", catchupStart,
                    catchupEnd, (catchupEnd-catchupStart+1), currentIndex, catchupDone, reProposeDone, msg.getSender());

            network.sendMessage(Configuration.clientIDs.get(1), new InfoMessage(myID,
                    "new leader reProposeLen(M)=" + (catchupEnd - catchupStart + 1) / 1000000.0));

            if (catchupEnd >= catchupStart) {
                //pre-init quorum entries or else leaderProcessAccepted update firstUn might not work
                //NOT init all here because catchupLen might be too large
                long modifyEnd = Math.min(catchupEnd, catchupStart+syncAccChunkSize*maxPendingCatchup);
                for (long i = catchupStart; i <= modifyEnd; i++)
                    modifyAcceptQuorumsEntry(i, true);
                LOG.info("gen acceptQuorum [%d, %d] catchupSendDone=%b\n", catchupStart, modifyEnd, catchupSendDone);

                int chunks = (int) ((catchupEnd - catchupStart + syncAccChunkSize) / syncAccChunkSize);
                int sendChunks = Math.min(chunks, maxPendingCatchup);
                int tsDrift = 50000; //todo set to normal
                for (int s = 0; s < sendChunks; s++) {
                    long cKey = catchupStart + syncAccChunkSize * s;
                    Quorum qr = new Quorum(Configuration.numQuorumAcceptors - 1, 0);

                    for (int k : acceptSet.keySet()) {
                        if (k != myID.hashCode()) {//not send to itself
                            qr.qrmap.put(k, new QuorumEntry(false, System.currentTimeMillis()+tsDrift));
                        }
                    }
                    leaderCatchupQuorums.put(cKey, qr);
                    LOG.debug("new leader catchupQuorums add entry[%d], s=%d\n", cKey, s);
                    tsDrift += 2000;
                }

                SyncMessage sync;
                if (chunks <= maxPendingCatchup) {
                    catchupSendDone = true;
                    sync = new SyncMessage(myID, catchupStart, (int) (catchupEnd-catchupStart+1));
                } else {
                    sync = new SyncMessage(myID, catchupStart, maxPendingCatchup * syncAccChunkSize);
                    catchupStart += maxPendingCatchup * syncAccChunkSize;
                    LOG.info("next catchupStart %d catchupEnd %d\n", catchupStart, catchupEnd);
                }

                for (int k : acceptSet.keySet()) {
                    if (k != myID.hashCode()) {//not send to itself
                        network.sendMessage(new NodeIdentifier(k), sync);
                        LOG.info("new leader send %s =>%s\n", sync, new NodeIdentifier(k));
                    }
                }
            }
        }

    }

    private void leaderProcessBatchAccepted(BatchAcceptedMessage msg) {
        long tsStart = System.currentTimeMillis();
        long end = msg.getEnd();
        AcceptedMessage accepted;
        for(long i=msg.getStart(); i<=end;i++) {
            accepted = new AcceptedMessage(msg.getSender(), msg.getRound(), i);
            leaderProcessAccepted(accepted);
        }
        tsReProposeUp += (System.currentTimeMillis() - tsStart)/1000.0;
        LOG.info("finish batch process accepted (%d, %d) Rup=%.1f firstUn=%d\n",
                msg.getStart(), end, tsReProposeUp, firstUnstableIndex);
    }

    private void processWakeup(WakeupMessage msg) {

        pingAcceptorTracks.put(msg.getSender().hashCode(), System.currentTimeMillis());
        isActive = true;
        checkpointIndex = Math.max(msg.getCheckpoint(), checkpointIndex);
        acceptorCatchupEnd = msg.getMaxLogIndex();
        acceptorCatchupStart = Math.max(firstUnstableIndex, checkpointIndex + 1);
        firstUnstableIndex = Math.max(acceptorCatchupStart, acceptorCatchupEnd + 1);
        long tmpLastClean = firstUnstableIndex /maxAccLogCacheSize * maxAccLogCacheSize;
        lastLogCacheCleanIndex = Math.max(lastLogCacheCleanIndex, tmpLastClean);

        LOG.info("#receive wakeup ckPt=%d firstUn=%d lastClean=%d, follower catchupRange(%d, %d, len=%d)\n",
                checkpointIndex, firstUnstableIndex, lastLogCacheCleanIndex, acceptorCatchupStart, acceptorCatchupEnd,
                (acceptorCatchupEnd-acceptorCatchupStart+1));
    }

    private void initAllAcceptMsgCatchup() {
        if (leaderID == null) {
            LOG.error("error no leader\n");
            throw new RuntimeException("no leader for follower to catchup\n");
        }

        acceptorCatchupSendDone = true;
        tsSwitchFollowerStart = System.currentTimeMillis();
        if (acceptorCatchupEnd >= acceptorCatchupStart) {
            long initStart = acceptorCatchupStart;
            acceptorCatchupDone = false;
            int chunks = (int) (acceptorCatchupEnd-acceptorCatchupStart+syncChunkSize)/syncChunkSize;
            int sendCount = (chunks+maxPendingCatchup-1)/maxPendingCatchup;

            long cKey;
            int sendChunks, syncLen;
            int tsDrift = 80000; //80s
            SyncMessage sync;

            for (int i=0; i<sendCount; i++) {
                syncLen = maxPendingCatchup*syncChunkSize;
                sendChunks = maxPendingCatchup;

                tsDrift += 4000; //set timestamp drift for later sync messages
                for (int s = 0; s < sendChunks; s++) {
                    cKey = acceptorCatchupStart + syncChunkSize * s;
                    acceptorCatchupTrack.put(cKey, new AcceptMsgChunk(syncChunkSize, System.currentTimeMillis()+tsDrift));
                }
                sync = new SyncMessage(myID, acceptorCatchupStart, syncLen);

                acceptorCatchupStart += maxPendingCatchup*syncChunkSize;
                network.sendMessage(leaderID, sync);
            }
            int bSz = Configuration.batchingSize;
            LOG.info("=>initAllAcceptMsg F start %d, end %d, chunks=%d #send=%d ckPt=%d firstUn=%d\n",
                    initStart, acceptorCatchupEnd, chunks, sendCount, checkpointIndex, firstUnstableIndex);

            network.sendMessage(Configuration.clientIDs.get(1), new InfoMessage(myID,
                    "wakeup follower catchupLen(M)="+(acceptorCatchupEnd-initStart+1)*bSz/1000000.0));

        } else {
            acceptorCatchupDone = true;
        }
    }

    private void processPendingAcceptorSync() {
        //long tStart = System.nanoTime();
        long key = acceptSyncMsgBuf.firstKey();

        interactiveProcessAcceptorSync(acceptSyncMsgBuf.get(key));
        acceptSyncMsgBuf.remove(key);
        //catchupFDTotalTime += (System.nanoTime()-tStart)/1000000.0;

        int catchupLen = (int) (acceptorCatchupEnd - acceptorCatchupStart+1);
        int eachChunk = maxPendingCatchup * syncAccChunkSize;
        int granularity;
        if (enableBatching) {
            granularity = 10;
        } else {
            granularity = 1000;
        }

        if (isDynThresholdEnabled && key-acceptorCatchupStart > catchupLen*0.1 && 1 == key%eachChunk) {
            int origDThreshold = Configuration.catchupDecisionThrottle;
            double avgR = (System.currentTimeMillis() - tsSwitchFollowerStart)
                    * eachChunk/(key-acceptorCatchupStart);
            double avgE = Configuration.leaCatchupTimeCap * slackRatioAcc * 1000.0
                    * eachChunk/catchupLen; // timeCap with slackRatioAcc
            double tmpTh = 0;
            if (avgR > avgE + 5 && accCatchupThreshold > 0.3 *origDThreshold) {
                tmpTh = (double) accCatchupThreshold * Math.max(avgE / avgR, 0.7);
            } else if (avgE > avgR + 5 && accCatchupThreshold < 3 * origDThreshold) {
                tmpTh = (double) accCatchupThreshold * Math.min(1.5, avgE / avgR);
            }
            if (tmpTh > 0.2*origDThreshold) {
                accCatchupThreshold = Math.round((int) tmpTh / granularity) * granularity;
            }
            LOG.info("key=%d adjust accCatchupThreshold=%d avgR=%.2f, avgE=%.2f tmpTh=%.1f\n",
                        key, accCatchupThreshold, avgR, avgE, tmpTh);
        }

        LOG.info("process pending acceptor catchup st=%d firstUn=%d lastClean=%d\n",
                key, firstUnstableIndex, lastLogCacheCleanIndex);
    }

    private void interactiveProcessAcceptorSync(SyncMessage msg) {
        pingAcceptorTracks.put(msg.getSender().hashCode(), System.currentTimeMillis());
        NodeIdentifier receiver = msg.getSender();
        long syncEndIndex;
        syncEndIndex = maxLogIndex;
        int chunks = (msg.getLen()+syncAccChunkSize-1)/syncAccChunkSize;
        long msgStart = msg.getStart();
        long msgEnd;
        int len;
        long firstIdx = lastLogCacheCleanIndex + 1;

        for (int i=0; i<chunks; i++) {
            if (i<chunks-1) {
                msgEnd = msgStart + syncAccChunkSize - 1;
            } else { //last chunk
                msgEnd = msg.getStart() + msg.getLen() - 1;
            }
            if (msgStart >= firstIdx) { //missing log entries in memory
                int diffLen = (int) (syncEndIndex + 1 - msgStart);
                if (diffLen > 0) {
                    len = Math.min(diffLen, syncAccChunkSize);
                    LOG.info("[AM] ckPt=%d firstKey=%d syncEnd=%d reply(st=%d len=%d)\n",
                            checkpointIndex, firstIdx, syncEndIndex, msgStart, len);
                    sendAcceptorLogChunk(0, msgStart, len, false, receiver);
                } else {
                    //fake send, spur leader re-propose inside receiveAcceptorLogChunk()
                    sendAcceptorLogChunk(0, msgStart, 0, false, receiver);
                    checkpointIndex = Math.max(msgStart / Configuration.checkpointInterval *
                            Configuration.checkpointInterval, checkpointIndex);
                    firstUnstableIndex = Math.max(checkpointIndex + 1, firstUnstableIndex);
                    LOG.info("fake reset ckPt=%d, firstUn=%d\n", checkpointIndex, firstUnstableIndex);
                }

            } else if (msgStart <= checkpointIndex) { //ignore chunks before checkpointIndex
                //assume [msgStart, msgEnd] cannot span over checkpointIndex
                sendAcceptorLogChunk(0, msgStart, 0, true, receiver);
                LOG.info("fake ckPt %d SyncEnd %d reply<st %d len 0>\n", checkpointIndex, syncEndIndex, msgStart);
            } else { //between [checkpointIndex+1, firstIdx-1]
                if (msgEnd < firstIdx) {
                    // purely on disk
                    //todo assume catchup disk chunk divider of checkpointInterval
                    len = (int) (msgEnd - msgStart + 1);
                    sendAcceptorLogChunk(0, msgStart, len, true, receiver);
                    LOG.info("[AD] ckPt=%d firstKey=%d SyncEnd=%d reply(st=%d len=%d)\n",
                            checkpointIndex, firstIdx, syncEndIndex, msgStart, len);
                } else { //span disk and memory
                    len = (int) (firstIdx - msgStart);
                    sendAcceptorLogChunk(0, msgStart, len, true, receiver);
                    LOG.info("[PD](ckPt %d, firstKey %d) SyncEnd %d reply<st %d len %d>\n",
                            checkpointIndex, firstIdx, syncEndIndex, msgStart, len);
                    len = (int) (msgEnd - firstIdx + 1);
                    sendAcceptorLogChunk(0, firstIdx, len, false, receiver);
                    LOG.info("[PM](ckPt %d, firstKey %d) SyncEnd %d reply<st %d len %d>\n",
                            checkpointIndex, firstIdx, syncEndIndex, firstIdx, len);
                }
            }

            msgStart += syncAccChunkSize;
        }
    }

    private void interactiveProcessLearnerSync(SyncMessage msg) {
        pingLearnerTracks.put(msg.getSender().hashCode(), System.currentTimeMillis());
        NodeIdentifier receiver = msg.getSender();
        long syncEndIndex = maxLogIndex;
        int chunks = (msg.getLen()+syncChunkSize-1)/syncChunkSize;
        long msgStart = msg.getStart();
        long msgEnd;
        int len;
        long firstIdx = lastLogCacheCleanIndex + 1;

        for (int i=0; i<chunks; i++) {
            if (i<chunks-1) {
                msgEnd = msgStart + syncChunkSize - 1;
            } else { //last chunk
                msgEnd = msg.getStart() + msg.getLen() - 1;
            }
            if (msgStart >= firstIdx) { //missing log entries in memory
                int diffLen = (int) (syncEndIndex + 1 - msgStart);
                len = (int) (msgEnd - msgStart + 1);
                if (diffLen > 0) {
                    len = Math.min(diffLen, len);
                    LOG.info("[LAM] reply<st %d, len %d> ckPt %d LogCache.1stKey %d\n",
                            msgStart, len, checkpointIndex, firstIdx);
                    sendDecisionChunk(msgStart, len, false, receiver);
                } else {
                    sendDecisionChunk(msgStart, 0, false, receiver);
                    LOG.warning("warning fake send <st %d len 0>\n", msgStart);
                }

            } else if (msgStart <= checkpointIndex) { //ignore chunks before checkpointIndex
                //todo: check LFD as it should not happen, acceptor's checkpointIndex function as GCIndex
                sendDecisionChunk(msgStart, 0, true, receiver);
                LOG.warning("warn=>[LFD] ckPt %d SyncEnd %d reply<st %d len 0>\n", checkpointIndex, syncEndIndex, msgStart);
            } else { //between [checkpointIndex+1, firstIdx-1]
                ////long end = msgStart + msg.getLen() - 1;
                if (msgEnd < firstIdx) {// purely on disk
                    len = (int) (msgEnd - msgStart + 1);
                    sendDecisionChunk(msgStart, len, true, receiver);
                    LOG.debug("[LAD] reply<st %d, len %d> ckPt %d LogCache.1stKey %d\n",
                            msgStart, len, checkpointIndex, firstIdx);
                } else { //span disk and memory
                    len = (int) (firstIdx - msgStart);
                    sendDecisionChunk(msgStart, len, true, receiver);
                    LOG.info("[LPD](ckPt %d, firstKey %d) SyncEnd %d reply<st %d len %d>\n",
                            checkpointIndex, firstIdx, syncEndIndex, msgStart, len);
                    len = (int) (msgEnd - firstIdx + 1);
                    sendDecisionChunk(firstIdx, len, false, receiver);
                    LOG.info("[LPM](ckPt %d, firstKey %d) SyncEnd %d reply<st %d len %d>\n",
                            checkpointIndex, firstIdx, syncEndIndex, firstIdx, len);
                }
            }

            msgStart += syncChunkSize;
        }
    }

    private void sendDecisionChunk(long start, int num, boolean onDisk, NodeIdentifier rec) {
        if (num > 0) {
            List<byte[]> logChunks = new ArrayList<>();
            int bbLen = 0;
            long end = start + num - 1;
            AcceptMessage accept;
            DecisionMessage decision;
            //LOG.debug("sendDecisionChunk=>(st %d, len %d, end %d, onDisk %b, dst %s)\n",
            //        start, num, end, onDisk, rec.toString());

            if (onDisk) { //fetch from disk
                long fetchStart = System.nanoTime();
                if (null == accDecIterator || accDecIteratorPos >= start) {
                    accDecIterator = acceptorLog.iterator(start);
                    LOG.warning("warning reset accDecIterator stPos=%d\n", start);
                }
                accDecIteratorPos = start-1;
                long logIndex;
                int logRound;

                while (accDecIterator.hasNext()) {
                    accept = accDecIterator.next();
                    logIndex = accept.getIndex();
                    logRound = accept.getRound();
                    if (logIndex >= start && logRound == maxRound) {
                        decision = new DecisionMessage(myID, logIndex, accept.getRequest());
                        //cacheDecisions.put(index, decision);
                        logChunks.add(decision.toByteArray());
                        bbLen += decision.toByteArray().length;
                        accDecIteratorPos++;
                        //LOG.info("disk while idx %d, decPos %d, bbLen %d, %s\n", index, accDecIteratorPos, bbLen, accept);
                    }
                    if (logIndex >= end && logRound == maxRound) {
                        //accDecIteratorPos = end;
                        break;
                    }
                }
                int tmpSz = logChunks.size();
                if (logChunks.size() != num) {
                    logChunks.clear();
                    long retryStart = System.nanoTime();
                    accDecIterator = acceptorLog.iterator(start);
                    ////accDecIteratorPos = start;
                    while (accDecIterator.hasNext()) {
                        accept = accDecIterator.next();
                        logIndex = accept.getIndex();
                        logRound = accept.getRound();
                        if (logIndex >= start && logRound == maxRound) {
                            decision = new DecisionMessage(myID, logIndex, accept.getRequest());
                            logChunks.add(decision.toByteArray());
                            bbLen += decision.toByteArray().length;
                            accDecIteratorPos++;
                        }
                        if (logIndex >= end && logRound == maxRound) {
                            //accDecIteratorPos = end;
                            break;
                        }
                    }
                    LOG.warning("warning sendDecisionChunk st %d num %d, realSz %d, retrySz %d, pos %d, tm %.1f ms\n",
                            start, num, tmpSz, logChunks.size(), accDecIteratorPos, (System.nanoTime()-retryStart)/1000000.0);

                }
                double fetchTime = (System.nanoTime() - fetchStart)/1000000.0;
                if (fetchTime > 1000) {
                    LOG.info("LAD<st=%d, ed=%d, dPos %d> !fetchTm %.1f ms chunkSz %d len %d (ckPt %d, clean %d)\n",
                            start, end, accDecIteratorPos, fetchTime, logChunks.size(), bbLen, checkpointIndex, lastLogCacheCleanIndex);
                } else {
                    LOG.info("LAD<st=%d, ed=%d, dPos %d> fetchTm %.1f ms chunkSz %d len %d (ckPt %d, clean %d)\n",
                            start, end, accDecIteratorPos, fetchTime, logChunks.size(), bbLen, checkpointIndex, lastLogCacheCleanIndex);
                }
            } else { //fetch from memory
                for (long i = start; i <= end; i++) {
                    accept = acceptorLogCache.get(i);

                    if (accept != null) {
                        decision = new DecisionMessage(myID, i, accept.getRequest());
                        logChunks.add(decision.toByteArray());
                        bbLen += decision.toByteArray().length;
                    }
                }
            }

            int sz = logChunks.size();
            if (sz != num) {
                LOG.warning("warning sendDecisionChunk sz %d != num %d, ignore\n", sz, num);
            } else {
                byte[] chunk = new byte[bbLen + 4 * sz + 4];
                LOG.debug("create DecisionChunk byteLen %d, #entries %d\n", chunk.length, sz);

                ByteBuffer bb = ByteBuffer.wrap(chunk);
                bb.putInt(sz); //#DecisionLogEntries
                for (byte[] b : logChunks) {
                    bb.putInt(b.length);
                    bb.put(b);
                }
                network.sendMessage(rec, new LearnerCatchupMessage(myID, start, sz, chunk));
                LOG.info("send LearnerCatchupMessage<st %d, end %d #Dec %d> =>%s\n", start, end, sz, rec);
            }
        } else {
            network.sendMessage(rec, new LearnerCatchupMessage(myID, start, num, null));
            LOG.debug("send fake LearnerCatchupMessage<st %d, #Dec %d> =>%s\n", start, num, rec);
        }

    }

    private void processSync(SyncMessage msg) {
        NodeIdentifier.Role role = msg.getSenderRole();
        switch (role) {
            case ACCEPTOR:
                if (leaderID == null) {
                    if (msg.getLen() * msg.getStart() > 0) { //leader sync
                        leaderID = msg.getSender(); //todo FAKE set leaderID
                        LOG.warning("warning wrong %s leaderID=%s\n", msg, leaderID);
                    } else {
                        LOG.error("error receive unexpected %s\n", msg);
                        throw new RuntimeException("leaderID is null inside processSync()");
                    }
                } else {
                    if (leaderID.equals(msg.getSender())) { // receive leader sync(0,-1)
                        if (-1 == msg.getLen()) { // L->F start follower catchup
                            initAllAcceptMsgCatchup();
                            LOG.info("##follower catchup start=%d end=%d\n", acceptorCatchupStart, acceptorCatchupEnd);
                        } else { // L->F rePropose
                            interactiveProcessAcceptorSync(msg);
                        }

                    } else { // F->L follower catchup
                        acceptSyncMsgBuf.put(msg.getStart(), msg);
                    }
                }
                break;

            case LEARNER:
                if (-1 == msg.getLen()) { // lc4r
                    leaCatchupDStart = msg.getStart()+1;
                    //network.sendMessage(Configuration.clientIDs.get(1), new InfoMessage(myID, "lc4r"));
                    LOG.info("lc4r reset leaCatchupDStart=%d\n", leaCatchupDStart);
                    catchupFetchDecisionTime = System.nanoTime();
                    catchupFDTotalTime = 0L;

                    if (blockedSwapInLearners.contains(msg.getSender().hashCode())) {
                        learnerActiveSet.put(msg.getSender().hashCode(), 0);
                        blockedSwapInLearners.remove(msg.getSender().hashCode());
                        LOG.debug("=>enable sending new decisions=>%s blocked=%s firstUn %d\n", msg.getSender(),
                                Arrays.toString(blockedSwapInLearners.toArray()), firstUnstableIndex);
                    }
                    SyncMessage sync = new SyncMessage(myID, firstUnstableIndex - 1, 0);
                    leaCatchupDLen = firstUnstableIndex - leaCatchupDStart;
                    network.sendMessage(msg.getSender(), sync);
                    //network.sendMessage(Configuration.clientIDs.get(1), new InfoMessage(myID, "LCatchup5"));
                    LOG.info("lc5s leaCatchupD start=%d len=%d\n", leaCatchupDStart, leaCatchupDLen);

                } else if (-1 == msg.getStart()) { //learner sync(-1,0) indicating catchupDecisionFinish
                    learnerSyncMsgBuf.clear();
                    network.sendMessage(msg.getSender(), new LearnerCatchupMessage(myID, 0, 0, null)); //finAck
                    LOG.info("finish leaCatchup pure %.1f s, all %.1f s finAck\n", catchupFDTotalTime/1000.0,
                            (System.nanoTime()-catchupFetchDecisionTime)/1000000000.0);
                } else {
                    learnerSyncMsgBuf.put(msg.getStart(), msg);
                }
                break;

            case CLIENT:
                LOG.debug("receive client %s, isLeader %b\n", msg, isLeader);
                if (isLeader && leaderSelected) {
                    long maxReceived = maxRequestID.get(msg.getSender().hashCode());
                    network.sendMessage(msg.getSender(),
                            new SyncClientReplyMessage(myID, maxReceived));
                }
                break;

            default:
                LOG.debug("leader receive unexpected SyncMessage\n");
                break;
        }
    }

    private void sendAcceptorLogChunk(int opMode, long start, int num, boolean onDisk, NodeIdentifier rec) {
        if (num > 0) {
            List<byte[]> logChunks = new ArrayList<>();
            int bbLen = 0;
            long end = start + num - 1;
            AcceptMessage accept;

            if (onDisk) { //fetch from disk
                long fetchStart = System.nanoTime();
                if (null == accLogIterator || start <= accLogIteratorPos) {
                    accLogIterator = acceptorLog.iterator(start);
                    LOG.info("sendAcceptorLogChunk init accLogIterator start=%d accLogIteratorPos=%d\n",
                            start, accLogIteratorPos);
                }
                accLogIteratorPos = start-1;
                long logIndex;
                int logRound;

                while (accLogIterator.hasNext()) {
                    accept = accLogIterator.next();
                    logIndex = accept.getIndex();
                    logRound = accept.getRound();
                    if (logIndex >= start && logRound == maxRound) {
                        logChunks.add(accept.toByteArray());
                        bbLen += accept.toByteArray().length;
                        accLogIteratorPos++;
                    }
                    if (logIndex >= end && logRound == maxRound)
                        break;
                    //LOG.debug("==> disk while index %d, bbLen %d, %s\n", index, bbLen, accept);
                }
                double fetchTime = (System.nanoTime() - fetchStart)/1000000.0;
                LOG.info("send acceptorLogChunk start=%d, end=%d, itPos=%d time=%.2f(ms)\n",
                        start, end, accLogIteratorPos, fetchTime);
            } else { //fetch from memory
                for (long i = start; i <= end; i++) {
                    accept = acceptorLogCache.get(i);

                    if (accept != null) {
                        logChunks.add(accept.toByteArray());
                        bbLen += accept.toByteArray().length;
                        //LOG.debug("=>memory for [index %d, req# %d:%d, bbLen %d\n",
                        //        i, accept.getClientIDHash(), accept.getClientReqNum(), bbLen);
                    }
                }
            }

            int sz = logChunks.size();
            if (sz != num) {
                LOG.warning("warning sendAcceptorLogChunk st %d, num %d, realSz %d\n", start, num, sz);
            } else {
                byte[] chunk = new byte[bbLen + 4 * sz + 4];
                LOG.info("create AcceptorLogChunk byteLen %d, #entries %d\n", chunk.length, sz);

                ByteBuffer bb = ByteBuffer.wrap(chunk);
                bb.putInt(sz); //for #AcceptorLogEntries
                for (byte[] b : logChunks) {
                    bb.putInt(b.length);
                    bb.put(b);
                }
                network.sendMessage(rec, new AcceptorCatchupMessage(myID, opMode, start, sz, chunk));
                LOG.info("send acceptorCatchup<start %d, end %d, #accept %d>=>%s\n", start, end, sz, rec);
            }
        } else {
            AcceptorCatchupMessage catchup =
                    new AcceptorCatchupMessage(myID, opMode, start, num, null);
            network.sendMessage(rec, catchup);
            LOG.info("send fake %s=>%s\n", catchup, rec);
        }

    }

    private void receiveAcceptorLogChunk(int opMode, byte[] chunk, long start, int len) {
        if (chunk == null) {
            if (opMode == 1) { //leader Merge
                AcceptMessage acm;
                for (long i=start; i<start+len; i++) {
                    acm = acceptorLogCache.get(i);
                    acm.setFirstUnstable(firstUnstableIndex); //todo check vdr ignore protection
                    acm.setSender(myID.hashCode());
                    acm.setRound(maxRound);
                    acceptorLogCache.put(i, acm); // todo protect acceptLogCache from cleanup during rePropose
                }
                LOG.info("set acceptorLogCache st=%d, len=%d, rPLastCache=%d\n", start, len, reProposeLastCacheIndex);
            }
            // else skip
        } else {
            ByteBuffer bb = ByteBuffer.wrap(chunk);
            int sz = bb.getInt();
            byte[] str;
            LOG.info("receiveAcceptorLogChunk opMode=%d, chunkSz=%d, firstUn=%d, rPLastCache=%d\n",
                    opMode, sz, firstUnstableIndex, reProposeLastCacheIndex);
            AcceptMessage msg;
            if (opMode == 1) { //new leader merge acceptMsg
                for (int i = 0; i < sz; i++) {
                    str = new byte[bb.getInt()];
                    bb.get(str);
                    msg = new AcceptMessage();
                    msg.readFromByteArray(str);
                    //LOG.debug("update req[%d:%d]=>log[%d]\n", msg.getClientIDHash(), msg.getClientReqNum(), msg.getIndex());
                    LOG.debug("update logIndex=%d\n", msg.getIndex());
                    //if (isLeader && !reProposeDone) {
                    long m = msg.getIndex();
                    if (acceptorLogCache.get(m) == null || (acceptorLogCache.get(m) != null &&
                            acceptorLogCache.get(m).getRound() < msg.getRound())) {
                        //solve fall behind new leader to merge bug
                        msg.setSender(myID.hashCode());
                        msg.setFirstUnstable(firstUnstableIndex); //todo check vdr ignore protection
                        msg.setRound(maxRound);

                        acceptorLogCache.put(m, msg); // todo protect acceptLogCache from cleanup during rePropose
                        LOG.debug("=>merge [%d] %s\n", m, msg);
                    } else {
                        LOG.debug("=>not merge as [%s] exists\n", acceptorLogCache.get(m));
                    }
                }
                ////acceptorLog.sync();

            } else if (opMode == 2) { //follower fast catchup
                for (int i = 0; i < sz; i++) {
                    str = new byte[bb.getInt()];
                    bb.get(str);
                    msg = new AcceptMessage();
                    msg.readFromByteArray(str);
                    //acceptorLog.update(msg); //todo check
                    catchupAccLog.update(msg);
                }
            } else { //leader rePropose
                for (int i = 0; i < sz; i++) {
                    str = new byte[bb.getInt()];
                    bb.get(str);
                    msg = new AcceptMessage();
                    msg.readFromByteArray(str);
                    if (sz == 1) {
                        processAccept(msg, 0);
                    } else { // sz > 1
                        if (i < sz - 1) {
                            processAccept(msg, 1); //batchMode cache
                        } else {
                            processAccept(msg, 2); //batchMode flush
                        }
                    }
                }
            }
        }
    }

    private void prefetchAcceptorLogToCache(long start) {
        long tsStart = System.currentTimeMillis();
        long end = start + syncAccChunkSize*maxPendingCatchup*5-1; //prefetch 5 chunks
        end = Math.min(end, maxLogIndex);

        for (long q = start; q <= end; q++)
            modifyAcceptQuorumsEntry(q, true);

        if (start > lastLogCacheCleanIndex) {
            LOG.info("NEED NOT prefetch start=%d, lastLogClean=%d\n", start, lastLogCacheCleanIndex);
            return;
        }

        if (null == accLogIterator || start <= accLogIteratorPos) {
            accLogIterator = acceptorLog.iterator(start);
            LOG.info("prefetch init accLogIterator start=%d, itPos=%d\n", start, accLogIteratorPos);
        }
        AcceptMessage accept;
        accLogIteratorPos = start-1;
        while (accLogIterator.hasNext()) {
            accept = accLogIterator.next();
            long index = accept.getIndex();
            if (index >= start) {
                acceptorLogCache.put(index, accept);
                accLogIteratorPos++;
            }
            if (index >= end)
                break;
        }

        if (firstUnstableIndex - reProposeLastCacheCleanIndex > maxAccLogCacheSize/ 2) {
            reProposeLastCacheCleanIndex += maxAccLogCacheSize / 2;
            acceptorLogCache.headMap(reProposeLastCacheCleanIndex).clear();
            LOG.info("cleanupB accLogCache rPLastCache=%d rPLastCacheClean=%d lastLogClean=%d, cacheSize=%d\n",
                    reProposeLastCacheIndex, reProposeLastCacheCleanIndex, lastLogCacheCleanIndex, acceptorLogCache.size());
        }
        // update reProposeLastCacheIndex
        reProposeLastCacheIndex = accLogIteratorPos + 1;


        tsPrefetch += (System.currentTimeMillis()-tsStart)/1000.0;
        LOG.info("prefetch (st=%d, end=%d), rPLast=%d, rPLastClean=%d lastLogClean=%d, tsP=%.0f(s)\n", start, end,
                reProposeLastCacheIndex, reProposeLastCacheCleanIndex, lastLogCacheCleanIndex, tsPrefetch);
    }

    private void leaderRePropose(long start, long to) {
        long end = Math.min(catchupEnd, to); //<== adjust last partial chunk
        LOG.info("acceptSet=%s learnerActive=%s rePropose(%d, %d) reProposeDone=%b\n", acceptSet.keySet(),
            learnerActiveSet.keySet(), start, end, reProposeDone);
        if (reProposeDone || start > end) {
            LOG.warning("warning reProposeDone=%b, start=%d, end=%d\n", reProposeDone, start, end);
            return;
        }
        for (int k : acceptSet.keySet()) {
            sendAcceptorLogChunk(1, start, (int) (end-start+1), false, new NodeIdentifier(k));
            LOG.info("new leader rePropose(%d, %d) => %s\n", start, end, new NodeIdentifier(k));
        }
    }

    private void processFreeLog(FreeLogMessage msg) {
        //pingTracks.put(msg.getSender().hashCode(), System.currentTimeMillis());
        LOG.info("#process %s ckPt=%d\n", msg, checkpointIndex);
        long ckIndex = msg.getIndex();
        int senderHash = msg.getSender().hashCode();
        int swapOutLearner = msg.getSwapOutLearner();

        if (swapOutLearner > 0) {
            // cleanup FreeLogMsg related to swapOut learner
            for (long k : freeAcceptorLogQuorum.keySet()) {
                if (freeAcceptorLogQuorum.get(k).contains(swapOutLearner)) {
                    freeAcceptorLogQuorum.get(k).remove(swapOutLearner);
                    LOG.info("=>cleanup freeLog msg from %s, remain %d:%s\n",
                            new NodeIdentifier(swapOutLearner), k, freeAcceptorLogQuorum.get(k));
                }
            }

        } else if (ckIndex > checkpointIndex) {
            // if (!isActive) { //todo check whether ignoring hurts correctness
            //     LOG.warning("warning isActive=%b ignore %s, ckPt=%d\n", isActive, msg, checkpointIndex);
            //     return;
            // }

            if (!freeAcceptorLogQuorum.containsKey(ckIndex)) {
                freeAcceptorLogQuorum.put(ckIndex, new HashSet<Integer>());
            }

            if (!freeAcceptorLogQuorum.get(ckIndex).contains(senderHash)) {
                // fill all possible ckPt slots
                for (long s : freeAcceptorLogQuorum.headMap(ckIndex, true).keySet()) {
                    if (!freeAcceptorLogQuorum.get(s).contains(senderHash)) {
                        freeAcceptorLogQuorum.get(s).add(senderHash);
                        LOG.info("add freeAcceptorLogQuorum.%d => %s\n", s, freeAcceptorLogQuorum.get(s));
                    }
                }

                // reverse scan [ckIndex, firstKey] and search maximum ckPt
                long expectedCkPt = 0;
                for (long rk : freeAcceptorLogQuorum.headMap(ckIndex, true).descendingKeySet()) {
                    if (freeAcceptorLogQuorum.get(rk).size() >= Configuration.numQuorumLearners) {
                        expectedCkPt = rk;
                        LOG.info("ckIndex=%d, h(sender)=%d, hSet %s, ckPt=%d <= rk=%d switchAccDone=%b break\n",
                                ckIndex, senderHash, freeAcceptorLogQuorum.get(rk), checkpointIndex,
                                expectedCkPt, switchAcceptorDone);
                        break;
                    }
                }

                //check whether receive majority learner FreeLogMessage
                if (expectedCkPt > checkpointIndex && switchAcceptorDone) {
                //if (freeAcceptorLogQuorum.get(ckIndex).size() >= Configuration.numQuorumLearners) {
                    //checkpointIndex = ckIndex;
                    checkpointIndex = expectedCkPt;
                    lastLogCacheCleanIndex = Math.max(checkpointIndex, lastLogCacheCleanIndex);
                    firstUnstableIndex = Math.max(firstUnstableIndex, checkpointIndex + 1);

                    if (isLeader && !catchupDone && checkpointIndex + 1 > catchupStart) {
                        catchupStart = checkpointIndex + 1;
                        if (catchupStart > catchupEnd) {
                            catchupDone = true;
                            LOG.info("<+>processFreeLog reset catchupDone=%b\n", catchupDone);
                        }

                        //cleanup leaderCatchupQuorums
                        int sizeA = leaderCatchupQuorums.size();
                        Iterator<Map.Entry<Long, Quorum>> iterator = leaderCatchupQuorums.entrySet().iterator();
                        while (iterator.hasNext()) {
                            Map.Entry<Long, Quorum> entry = iterator.next();
                            if (entry.getKey() < catchupStart)
                                iterator.remove();
                        }
                        //cleanup acceptQuorums
                        int sizeB = acceptQuorums.size();
                        iterator = acceptQuorums.entrySet().iterator();
                        while (iterator.hasNext()) {
                            Map.Entry<Long, Quorum> entry = iterator.next();
                            if (entry.getKey() < catchupStart)
                                iterator.remove();
                        }
                        LOG.info("<+>cleanup leaderCatchQr %d=>%d, acceptQr %d=>%d, catchupDone=%b, catchup(%d, %d)\n",
                                sizeA, leaderCatchupQuorums.size(), sizeB, acceptQuorums.size(), catchupDone,
                                catchupStart, catchupEnd);
                    }

                    if (!isLeader && isActive &&
                        !acceptorCatchupDone && checkpointIndex + 1 > acceptorCatchupStart) {
                        acceptorCatchupStart = checkpointIndex + 1;
                        if (acceptorCatchupStart > acceptorCatchupEnd) {
                            acceptorCatchupDone = true;
                            double endTm = (System.currentTimeMillis()-tsSwitchFollowerStart)/1000.0;
                            network.sendMessage(Configuration.clientIDs.get(1),
                                    new InfoMessage(myID, "<+>finish switch follower time(s)=" + endTm));

                            acceptorLog.moveLog();
                            //todo send to ALL if leaderID=null
                        }

                        //cleanup acceptorCatchupTrack
                        acceptorCatchupTrack.headMap(acceptorCatchupStart).clear();

                        if (!acceptorCatchupDone) {
                            LOG.info("cleanup accCatchQr=>%d catchup(start=%d, end=%d)\n", acceptorCatchupTrack.size(),
                                    acceptorCatchupStart, acceptorCatchupEnd);
                        }
                    }

                    long tmA = System.nanoTime();
                    //acceptorLog.sync(); //disable for now
                    acceptorLog.garbageCollect(checkpointIndex);
                    accLogIterator = null;
                    accDecIterator = null;
                    accLogIteratorPos = 0;
                    accDecIteratorPos = 0;
                    double gcLogTime = (System.nanoTime() - tmA) / 1000000.0;
                    long tmC = System.nanoTime();
                    freeAcceptorLogQuorum.headMap(checkpointIndex + 1).clear();
                    acceptorLogCache.headMap(checkpointIndex + 1).clear();
                    double freeCacheTime = (System.nanoTime() - tmC) / 1000000.0;

                    if (gcLogTime > Configuration.acceptTimeout / 2) {
                        LOG.warning("warn GC<=%d %.2f ms, freeLogCache %.2f ms accQr=%d aLogCache=%d\n",
                                checkpointIndex, gcLogTime, freeCacheTime, acceptQuorums.size(), acceptorLogCache.size());
                    } else {
                        LOG.info("GC<=%d %.2f ms, freeLogCache %.2f ms accQr=%d aLogCache=%d\n",
                                checkpointIndex, gcLogTime, freeCacheTime, acceptQuorums.size(), acceptorLogCache.size());
                    }
                    // Configuration.displayMemoryInfo(myID.toString());
                    //if (isLeader)
                    //    showReqCountInfo();
                }
            } else {
                LOG.warning("freeAcceptorLogQuorum already get %d, HashSet %s\n", senderHash,
                        freeAcceptorLogQuorum.get(ckIndex));
            }
        } else {
            LOG.warning("freeLog (src=%s, ckIndex=%d) <= ckPtIdx %d\n", msg.getSender(), ckIndex, checkpointIndex);
        }
    }

    private void leaderCatchup(AcceptorCatchupMessage msg) {
        long ck = msg.getFrom();
        if (catchupDone || leaderCatchupQuorums.get(ck) == null) {
            LOG.info("leaderCatchupQuorum did entry[%d] before, leader catchupDone %b\n", ck, catchupDone);
        } else {
            LOG.info("leader catchup ckPt=%d, rPLast=%d, lastClean=%d\n", ck,
                    reProposeLastCacheIndex, lastLogCacheCleanIndex);
            // Configuration.displayMemoryInfo(myID.toString());
            if (ck >= reProposeLastCacheIndex) {
                prefetchAcceptorLogToCache(ck);
            }

            // if (ck <= lastLogCacheCleanIndex && ck >= reProposeLastCacheIndex) {
            //     prefetchAcceptorLogToCache(ck);
            // }
            long tsStart = System.currentTimeMillis();
            if (msg.getNumOfAccepts() > 0) {
                receiveAcceptorLogChunk(1, msg.getAcceptsBytes(), ck, msg.getNumOfAccepts());
            } else {
                receiveAcceptorLogChunk(1, null, ck, (int) Math.min(syncAccChunkSize,catchupEnd-ck+1));
            }
            tsMerge += (System.currentTimeMillis()-tsStart)/1000.0;

            Quorum qr = leaderCatchupQuorums.get(ck);
            qr.incQuorum(); //todo bug: increase if receives catchupMsg from different acceptor
            if (qr.recvAll()) {
                leaderCatchupQuorums.remove(ck);
                tsStart = System.currentTimeMillis();
                leaderRePropose(ck, ck + syncAccChunkSize - 1);
                tsReProposeS += (System.currentTimeMillis()-tsStart)/1000.0;
                LOG.info("catchupQuorum[%d] get all (P=%.1f, M=%.1f, Rs=%.1f, Rr=%.1f Rup=%.1f)s firstUn=%d\n",
                        ck, tsPrefetch, tsMerge, tsReProposeS, tsReProposeR, tsReProposeUp, firstUnstableIndex);

                //send new batch of syncMsg
                if (!catchupSendDone && leaderCatchupQuorums.size() < maxPendingCatchup/2) {
                    if (catchupEnd >= catchupStart) {
                        int chunks = (int) ((catchupEnd-catchupStart + syncAccChunkSize) / syncAccChunkSize);
                        int sendChunks = Math.min(chunks, maxPendingCatchup);
                        int tsDrift = 2000;
                        for (int s = 0; s < sendChunks; s++) {
                            long cKey = catchupStart + syncAccChunkSize * s;
                            Quorum quorum = new Quorum(Configuration.numQuorumAcceptors-1, 0);

                            for (int k : acceptSet.keySet()) {
                                if (k != myID.hashCode()) {//not send to itself
                                    quorum.qrmap.put(k, new QuorumEntry(false, System.currentTimeMillis()+tsDrift));
                                }
                            }
                            leaderCatchupQuorums.put(cKey, quorum);
                            LOG.info("new leader catchupQuorums add entry[%d], s=%d\n", cKey, s);
                            tsDrift += 2000;
                        }

                        SyncMessage sync;
                        if (chunks <= maxPendingCatchup) {
                            catchupSendDone = true;
                            sync = new SyncMessage(myID, catchupStart, (int)(catchupEnd-catchupStart+1));
                        } else {
                            sync = new SyncMessage(myID, catchupStart, maxPendingCatchup * syncAccChunkSize);
                            catchupStart += maxPendingCatchup * syncAccChunkSize;
                            LOG.info("next catchupSendStart %d #chunks %d Unstable %d MaxLogIdx %d\n",
                                    catchupStart, chunks, firstUnstableIndex, maxLogIndex);
                        }

                        for (int k : acceptSet.keySet()) {
                            if (k != myID.hashCode()) {//not send to itself
                                network.sendMessage(new NodeIdentifier(k), sync);
                                LOG.info("new leader send batch %s =>%s\n", sync, new NodeIdentifier(k));
                            }
                        }
                    }
                }
            } else {
                leaderCatchupQuorums.put(ck, qr);
            }

            if (catchupSendDone && leaderCatchupQuorums.isEmpty()) {
                catchupDone = true;
                LOG.info("##catchupDone=%b, reProposeDone=%b\n", catchupDone, reProposeDone);
            }

            //(1B) check catchupDone
            if (!leaderCatchupQuorums.isEmpty() && !catchupDone) {
                Iterator<Map.Entry<Long, Quorum>> iterator = leaderCatchupQuorums.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<Long, Quorum> entry = iterator.next();
                    qr = entry.getValue();
                    Iterator<Map.Entry<Integer, QuorumEntry>> it = qr.qrmap.entrySet().iterator();

                    SyncMessage sync = new SyncMessage(myID, entry.getKey(), syncAccChunkSize);
                    while (it.hasNext()) {
                        Map.Entry<Integer, QuorumEntry> subEntry = it.next();
                        if (System.currentTimeMillis() - subEntry.getValue().getTimeStamp()
                                > Configuration.acceptTimeout) {
                            network.sendMessage(new NodeIdentifier(subEntry.getKey()), sync);

                            qr.qrmap.put(subEntry.getKey(),
                                    new QuorumEntry(false, System.currentTimeMillis()));
                            LOG.info("1B leaderCatchup resend %s reset quorum %d entry %d\n",
                                    sync, entry.getKey(), subEntry.getKey());
                        }
                    }
                    leaderCatchupQuorums.put(entry.getKey(), qr);
                }
            }
        }
    }

    private void acceptorCatchup(AcceptorCatchupMessage msg) {
        long ck = msg.getFrom();
        if (acceptorCatchupDone) {
            acceptorCatchupTrack.clear();
            LOG.info("accCatchupDone=%b clear\n", ck, acceptorCatchupDone);
        } else if (!acceptorCatchupTrack.containsKey(ck) || acceptorCatchupTrack.get(ck) == null) {
            LOG.info("acceptorCatchupTrack did entry[%d] before\n", ck);
        } else {
            if (msg.getNumOfAccepts() > 0) {
                receiveAcceptorLogChunk(2, msg.getAcceptsBytes(), ck, msg.getNumOfAccepts());
                LOG.info("acceptor catchup (start=%d, len=%d) firstUn=%d lastClean=%d, logCacheSz=%d\n", ck,
                        msg.getNumOfAccepts(), firstUnstableIndex, lastLogCacheCleanIndex, acceptorLogCache.size());
            }
            acceptorCatchupTrack.remove(ck);

            if (acceptorCatchupSendDone && acceptorCatchupTrack.headMap(acceptorCatchupEnd+1).isEmpty()) {
                acceptorCatchupDone = true;
                double tm = (System.currentTimeMillis()-tsSwitchFollowerStart)/1000.0;
                network.sendMessage(Configuration.clientIDs.get(1),
                        new InfoMessage(myID, "finish switch follower time(s)="+ tm));

                acceptorLog.moveLog();
            }

            //(3B) check acceptor catchup
            if (!acceptorCatchupTrack.isEmpty() && !acceptorCatchupDone) {
                Iterator<Map.Entry<Long, AcceptMsgChunk>> iterator = acceptorCatchupTrack.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<Long, AcceptMsgChunk> entry = iterator.next();
                    if (System.currentTimeMillis() - entry.getValue().ts > Configuration.acceptTimeout/2) {
                        acceptorCatchupTrack.put(entry.getKey(), new AcceptMsgChunk(entry.getValue().len,
                                System.currentTimeMillis()));
                        SyncMessage syncD = new SyncMessage(myID, entry.getKey(), entry.getValue().len);
                        network.sendMessage(msg.getSender(), syncD);
                        LOG.info("3B acceptorCatchup resend %s\n", syncD);
                        break;  //break restricts send just one sync
                    }
                }
            }
        }
    }

    private void processAcceptCatchup(AcceptorCatchupMessage msg) {
        pingAcceptorTracks.put(msg.getSender().hashCode(), System.currentTimeMillis());

        if (1 == msg.getMode()) { //leader rePropose
            long tsStart = System.currentTimeMillis();
            receiveAcceptorLogChunk(0, msg.getAcceptsBytes(), msg.getFrom(), msg.getNumOfAccepts());
            tsReProposeR += (System.currentTimeMillis()-tsStart)/1000.0; //on both leader and followers
            LOG.debug("receive acceptorLogChunk with mode=%d\n", msg.getMode());
        } else {
            if (isLeader && leaderSelected) {
                leaderCatchup(msg);
            } else {
                acceptorCatchup(msg);
            }
        }
    }

    private void processSleepDown(SleepDownMessage msg) {
        pingAcceptorTracks.put(msg.getSender().hashCode(), System.currentTimeMillis());
        isActive = false;
        leaderID = msg.getSender(); //<= only leader will send sleepDown
        LOG.info("receive sleep down isActive=%b\n", isActive);
    }

	@Override
	public void handleMessage(Message msg) {
        long tsH = System.nanoTime();
        switch (Message.MSG_TYPE.values()[msg.getType()]) {
            case REQ:
                //todo opt try to relax condition
                if (isLeader && leaderSelected && reProposeDone) {
                    // if (leaCatchupDThreshold > 0) {
                    reqCount.addReceived(); //vdr profiler
                    /*if (enableFastPath) {
                        try {
                            while (!fAccQueue.isEmpty()) {
                                AcceptMessage acc = fAccQueue.take();
                                //if (!switchAcceptorDone)
                                //    LOG.info("+take %s and process\n", acc);
                                processAccept(acc, 0);
                            }
                            while (!fAccdQueue.isEmpty()) {
                                AcceptedMessage acd = fAccdQueue.take();
                                //if (!switchAcceptorDone)
                                //    LOG.info("-take %s and process\n", acd);
                                leaderProcessAccepted(acd);
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }*/
                    leaderProcessClientRequest((RequestMessage) msg);

                    // } else {
                    //     if (!learnerSyncMsgBuf.isEmpty()) {
                    //         processPendingLearnerSync();
                    //     }
                    //     countCycle++;
                    //     if (0 == countCycle%20) {
                    //         if (countCycle > 1000) {
                    //             countCycle = 1;
                    //         }
                    //         LOG.info("drop request due to leaCatchupDThreshold=%d\n", leaCatchupDThreshold);
                    //     }
                    // }
                } else {
                    reqCount.addRejected(); //vdr profiler
                    countCycle++;
                    if (0 == countCycle% 100) {
                        if (countCycle > 2000) {
                            countCycle = 1;
                        }
                        LOG.info("reject to process client requests rPDone=%b isL=%b selected=%b\n",
                                reProposeDone, isLeader, leaderSelected);
                    }
                    if (!isLeader && leaderSelected && leaderID != null) { //follower informs client
                        network.sendMessage(msg.getSender(), new PingMessage(myID, leaderID.hashCode()));
                    }
                }
                break;
            case PING:
                processPing((PingMessage) msg);
                break;
            case PREPARE:
                processPrepare((PrepareMessage) msg);
                break;
            case PROMISE:
                leaderProcessPromise((PromiseMessage) msg);
                break;
            case ACCEPT:
                LOG.debug("ACC %s\n", (AcceptMessage) msg);
                processAccept((AcceptMessage) msg, 0);
                break;
            case ACCEPTED:
                LOG.debug("ACD %s\n", (AcceptedMessage) msg);
                leaderProcessAccepted((AcceptedMessage) msg);
                break;
            case BACCEPTED:
                leaderProcessBatchAccepted((BatchAcceptedMessage) msg);
                break;
            case FREELOG:
                processFreeLog((FreeLogMessage) msg);
                break;
            case WAKEUP:
                //LOG.debug("get --> %s\n", (WakeupMessage) msg);
                processWakeup((WakeupMessage) msg);
                break;
            case SYNC:
                //LOG.debug("get --> %s\n", (SyncMessage) msg);
                processSync((SyncMessage) msg);
                break;
            case ACATCHUP:
                //LOG.info("get->%s\n", (AcceptorCatchupMessage) msg);
                processAcceptCatchup((AcceptorCatchupMessage) msg);
                break;
            case SLEEPDOWN:
                processSleepDown((SleepDownMessage) msg);
                break;
            case DECISION:
                processFakeDecision((DecisionMessage) msg);
                break;
            default:
                LOG.error("error acceptor get unknown msg: " + msg);
                throw new RuntimeException("Learner received unsupported message\n");
        }

        msgProfiler.incMsgTiming(Message.MSG_TYPE.values()[msg.getType()], System.nanoTime() - tsH);
        msgProfiler.incHandleMsgTime(System.nanoTime()-tsH);
	}

    @Override
    public void handleTimer() {
        if (isLeader) {
            if (leaderSelected) {
                //(1A) check catchupDone => leaderCatchup()
                if (!catchupDone) {
                    if (!leaderCatchupQuorums.isEmpty()) {
                        Quorum qr;
                        Iterator<Map.Entry<Long, Quorum>> iterator = leaderCatchupQuorums.entrySet().iterator();
                        while (iterator.hasNext()) {
                            Map.Entry<Long, Quorum> entry = iterator.next();
                            qr = entry.getValue();
                            Iterator<Map.Entry<Integer, QuorumEntry>> it =
                                    qr.qrmap.entrySet().iterator();

                            while (it.hasNext()) {
                                Map.Entry<Integer, QuorumEntry> subEntry = it.next();
                                if (System.currentTimeMillis() - subEntry.getValue().getTimeStamp()
                                        > Configuration.acceptTimeout) {
                                    //todo: receiver need adjust last slice size
                                    SyncMessage sync = new SyncMessage(myID, entry.getKey(), syncAccChunkSize);
                                    network.sendMessage(new NodeIdentifier(subEntry.getKey()), sync);
                                    qr.qrmap.put(subEntry.getKey(),
                                            new QuorumEntry(false, System.currentTimeMillis()));
                                    LOG.info("1A catchupSend timeout, resend %s, reset quorum %d entry %d\n",
                                            sync, entry.getKey(), subEntry.getKey());
                                }
                            }
                            leaderCatchupQuorums.put(entry.getKey(), qr);
                        }
                    }
                    return;
                }

                if (!reProposeDone)
                    return;

                //(2A) check reProposeDone and accept/p2a timeout => leaderProcessAccepted()
                outLoop2A:
                if (/*reProposeDone && */ acceptQuorums.get(firstUnstableIndex) != null) {
                    Quorum qr = acceptQuorums.get(firstUnstableIndex);
                    if (null == qr || qr.qrmap.isEmpty())
                        return;
                    for (int k : qr.qrmap.keySet()) {
                        if (k != myID.hashCode()) { //todo check vdr
                            QuorumEntry entry = qr.qrmap.get(k);
                            if (entry.getTimeStamp() != 0 && !entry.getDone() &&
                                System.currentTimeMillis() - entry.getTimeStamp() > Configuration.acceptTimeout) {
                                LOG.info("2A notice %s %d: %s timeout, swap it out\n",
                                        (new NodeIdentifier(k)), firstUnstableIndex, entry);
                                leaderSwitchAcceptor(firstUnstableIndex, k);
                                break outLoop2A;
                            }
                        }
                    }
                }

                //learner decision timeout, handle it in passive way in case learner fails to
                //receive one decision, the learner will send SyncMessage to leader
            } else {
                //check prepare/p1a timeout => no
                if (!prepareQuorum.recvQuorum()) {
                    for (int i : prepareQuorum.qrmap.keySet()) {
                        QuorumEntry prep = prepareQuorum.qrmap.get(i);
                        if (!prep.getDone() && prep.getTimeStamp() != 0
                                && System.currentTimeMillis() - prep.getTimeStamp()
                                > Configuration.prepareTimeout) {
                            prep.setTimeStamp(System.currentTimeMillis());
                            prepareQuorum.qrmap.put(i, prep);
                            LOG.info("%s resend prepare/p1a msg to %s\n", myID, new NodeIdentifier(i));
                            network.sendMessage(new NodeIdentifier(i),
                                    new PrepareMessage(myID, maxRound, checkpointIndex, firstUnstableIndex));
                        }
                    }
                }
            }
        } else { //other acceptor
            //exit if new leader has not finished re-propose
            if (!reProposeDone)
                return;

            //(3A) check acceptor catchup => acceptorCatchup()
            if (!acceptorCatchupDone) {
                if (!acceptorCatchupTrack.isEmpty()) {
                    Iterator<Map.Entry<Long, AcceptMsgChunk>> iterator = acceptorCatchupTrack.entrySet().iterator();
                    int tsDrift = 10000;
                    int count = 0;
                    while (iterator.hasNext() && count < maxPendingCatchup/2) {
                        Map.Entry<Long, AcceptMsgChunk> entry = iterator.next();
                        if (System.currentTimeMillis() - entry.getValue().ts > Configuration.acceptTimeout/2) {
                            int mLen = entry.getValue().len;
                            acceptorCatchupTrack.put(entry.getKey(), new AcceptMsgChunk(mLen,
                                    System.currentTimeMillis() + tsDrift));
                            SyncMessage syncD = new SyncMessage(myID, entry.getKey(), mLen);
                            network.sendMessage(leaderID, syncD); //todo check vdr
                            LOG.info("3A check acceptor catchup timeout resend %s\n", syncD);
                            tsDrift += 3000;
                            count++;
                        }
                    }
                }
                return;
            }

            //acceptor in acceptSet check ping timeout => no
            for (int k : pingAcceptorTracks.keySet()) {
                if (k>>24 != NodeIdentifier.Role.ACCEPTOR.ordinal() || k == myID.hashCode())
                    continue;
                long diff = System.currentTimeMillis() - pingAcceptorTracks.get(k);
                if (diff > Configuration.acceptTimeout) {  //todo use shorter timeout
                    if (leaderID != null && k == leaderID.hashCode()) { //todo check vdr
                        LOG.info("acceptor %s cannot receive leader=%s ping\n", myID, leaderID);
                        startLeaderElection();
                    } else {
                        //LOG.debug("pingTracks remove %s\n", new NodeIdentifier(k));
                        //pingTracks.remove(k);
                    }
                }
            }
        }
	}

    private class PingTimer extends TimerTask {
        //private long reqCountCycle = 0L;

        @Override
        public void run() {
            // reqCountCycle++;
            // if (isLeader && 0 == reqCountCycle%20) { //assume pingInterval 200 ms
            //     showReqCountInfo();
            //     showClientReqInfo();
            // }

            long info = 0L;
            for (int k : pingAcceptorTracks.keySet()) {
                if (k == myID.hashCode())  //do not send to itself
                    continue;
                if (leaderSelected && isLeader) {
                    if (!reProposeDone)
                        info = -1;
                } else {
                    info = (leaderID == null) ? -2 : maxRound;
                }

                //skip timeout acceptor
                if (System.currentTimeMillis() - pingAcceptorTracks.get(k) < Configuration.acceptTimeout) {
                    NodeIdentifier dst = new NodeIdentifier(k);
                    network.sendMessage(dst, new PingMessage(myID, info));
                } else {
                    pingAcceptorTracks.put(k, System.currentTimeMillis());
                    LOG.debug("notice %s timeout, reset timestamp\n", new NodeIdentifier(k));
                }

            }

            for (int m : pingLearnerTracks.keySet()) {
                if (System.currentTimeMillis() - pingLearnerTracks.get(m) < Configuration.learnerTimeout) {
                    info = (leaderID == null) ? 0 : leaderID.hashCode();
                    network.sendMessage(new NodeIdentifier(m), new PingMessage(myID, info));
                } else {
                    pingLearnerTracks.put(m, System.currentTimeMillis());
                    if (!Configuration.enableFailDetection &&
                        leaderSelected && isLeader && learnerActiveSet.containsKey(m)) {
                        LOG.info("fast try switch learner %d\n", m);
                        boolean succeed = leaderSwitchLearner(m);
                        if (succeed) {
                            pingLearnerTracks.remove(m);
                            LOG.warning("notice %s timeout remain pingTracks %s, learnerActiveSet %s\n",
                                    new NodeIdentifier(m), pingLearnerTracks.keySet(), learnerActiveSet.keySet());
                        }
                    } //todo SHOULD enable (add reProposeDone condition?)
                }
            }
        }
    }

	@Override
	public void handleFailure(NodeIdentifier node, Throwable cause) {
        if (!Configuration.enableFailDetection) {
            LOG.debug("AFailure %s:%s\n", node, cause);
            return;
        }

        if (cause instanceof ClosedChannelException) {
            ltoTimestamp = System.currentTimeMillis();
            LOG.warning(" => catch %s ClosedChannelException ltoTimestamp=%d\n", node, ltoTimestamp);
            int nodeHash = node.hashCode();
            if (pingLearnerTracks.containsKey(nodeHash)) {
                pingLearnerTracks.remove(nodeHash);
                LOG.info("remove %s from pingLearnerTracks remain %s\n", node, pingLearnerTracks.keySet());
                if (learnerActiveSet.containsKey(nodeHash)) {
                    //learnerActiveSet.remove(nodeHash);
                    //LOG.info("remove %d from learnerActiveSet remain %s\n", nodeHash, learnerActiveSet.keySet());

                    if (leaderSelected && isLeader) {
                        boolean succeed = leaderSwitchLearner(nodeHash);
                        if (succeed) {
                            LOG.info("catch %s exception learnerActiveSet remain %s\n", node, learnerActiveSet.keySet());
                        } else {
                            LOG.warning("warning swap out %s failed\n", node);
                        }
                    }
                } else {
                    LOG.warning("warning %s not in learnerActiveSet\n", node);
                }

                if (learnerInActiveSet.containsKey(nodeHash)) {
                    //int idx = learnerInActiveSet.indexOf(nodeHash);
                    //learnerInActiveSet.remove(idx);
                    learnerInActiveSet.remove(nodeHash);
                    LOG.info("remove %d from learnerInActiveSet remain %s\n", nodeHash, learnerInActiveSet.keySet());
                } else {
                    LOG.warning("warning %s not in learnerInActiveSet\n", node);
                }

            } else if (pingAcceptorTracks.containsKey(nodeHash)) {
                pingAcceptorTracks.remove(nodeHash);
                LOG.info("remove %s from pingAcceptorTracks remain=%s\n", node, pingAcceptorTracks.keySet());
                //todo vdr profiling
                // msgProfiler.setProgramTime(System.currentTimeMillis());
                // msgProfiler.showMsgTiming();
                // //msgProfiler.showFuncTiming(Message.MSG_TYPE.REQ);
                network.sendMessage(Configuration.clientIDs.get(1), new InfoMessage(myID, "showMsgTiming"));

                // int idx;
                if (acceptSet.containsKey(nodeHash)) {
                    // idx = acceptSet.indexOf(nodeHash);
                    // acceptSet.remove(idx);
                    acceptSet.remove(nodeHash);
                    LOG.info("remove ak=%d acceptSet=%s backupSet=%s\n", nodeHash, acceptSet.keySet(), backupSet.keySet());
                } else {
                    LOG.warning("warning %s not in acceptSet=%s\n", node, acceptSet.keySet());
                }

                if (backupSet.containsKey(nodeHash)) {
                    // idx = backupSet.indexOf(nodeHash);
                    // backupSet.remove(idx);
                    backupSet.remove(nodeHash);
                    LOG.info("remove bk=%d acceptSet=%s backupSet=%s\n", nodeHash, acceptSet.keySet(), backupSet.keySet());
                } else {
                    LOG.warning("warning %s not in backupSet=%s\n", node, backupSet.keySet());
                }

                if (leaderID != null && leaderID.equals(node)) {
                    leaderID = null;
                    LOG.info("fast start leader election\n");
                    startLeaderElection();
                }

                if (leaderSelected && isLeader) {
                    if (backupSet.isEmpty()) {
                        LOG.warning("backupSet isEmpty=%b, abandon switch acceptor\n", backupSet.isEmpty());
                    } else {
                        acceptSet.put(nodeHash, 0);
                        LOG.info("fast switch acceptor\n");
                        leaderSwitchAcceptor(firstUnstableIndex, nodeHash); //todo check vdr need not protect
                    }
                }
            } else {
                LOG.debug("neither pingLearnerTracks or pingAcceptorTracks contains %s\n", node);
            }
        } else {
            LOG.debug(" ==>%s:%s\n", node, cause);
        }
	}

    private void leaderProcessClientRequest(RequestMessage msg) {

        int senderHash = msg.getSender().hashCode(); //key for maxReceivedRequestNum
        int senderID = msg.getSenderID(); //key for maxAcceptedRequestNum
        long requestNum = msg.getReqnum();
        //cut down ping overhead and pingTracks do not store clientID
        //pingTracks.put(senderHash, System.currentTimeMillis());

        long maxReceivedNum = maxRequestID.get(senderHash);
        long maxAcceptedNum = maxRequestID.get(senderID);

        LOG.debug("pReq receive %s maxReceived=%d maxAccepted=%d firstUn=%d\n", msg,
                maxReceivedNum, maxAcceptedNum, firstUnstableIndex);

        // requests from each client arrive in order, or at least leader process them in order
        if (requestNum > maxReceivedNum) {
            // long reqTs = System.nanoTime(); //func profiling

            TreeMap futureReq = futureRequests.get(senderHash);
            if (!futureReq.containsKey(requestNum)) {
                pendingReqCount++;
                futureReq.put(requestNum, msg); //todo check move this inside if() condition
                LOG.debug("pReq put req=%d => futureReq id=%d sz=%d pendingCnt=%d\n", requestNum, senderHash,
                        futureReq.size(), pendingReqCount);
            }

            // msgProfiler.incFuncTiming(MessageProfiler.FUNC_STAGE.S1, (System.nanoTime() - reqTs));
            // reqTs = System.nanoTime();

            if (/*reProposeDone &&*/ 0 == currentIndex % leaCatchupDThreshold) {
                // wait for >= 5min and leaCatchupLen >= 5/8 * ckPtInterval
                if ((ltoTimestamp > 0 && System.currentTimeMillis() - ltoTimestamp >= 300000) && // 5min
                        (!freeAcceptorLogQuorum.isEmpty() &&
                        firstUnstableIndex - freeAcceptorLogQuorum.lastKey() >= ckPtInterval * 5/8)) { // 5/8 interval

                    if (!blockedSwapInLearners.isEmpty()) {
                        WakeupMessage wakeup = new WakeupMessage(myID, checkpointIndex, firstUnstableIndex-1);
                        for (int tol : blockedSwapInLearners) {
                            network.sendMessage(new NodeIdentifier(tol), wakeup);    //LCatchup 1
                        }
                        network.sendMessage(Configuration.clientIDs.get(1), new InfoMessage(myID, "lc1s"));
                        LOG.info("lc1s elapseTime(s)=%.0f send %s blockedLea=%s\n",
                                (System.currentTimeMillis() - ltoTimestamp) / 1000.0, wakeup,
                                Arrays.toString(blockedSwapInLearners.toArray()));
                        //blockedSwapInLearners.clear(); //delete it in lc4r
                    }

                    if (!blockedSwapInAcceptors.isEmpty()) {
                        tsSwitchFollowerStart = System.currentTimeMillis(); // leader side
                        for (int toa : blockedSwapInAcceptors) {
                            //NodeIdentifier toAcc = new NodeIdentifier(to);
                            //network.sendMessage(toAcc, new WakeupMessage(myID,
                            //        acceptorCatchupStart, acceptorCatchupEnd));
                            network.sendMessage(new NodeIdentifier(toa), new SyncMessage(myID, 0, -1));
                        }
                        LOG.info("startF blockedAcc=%s ckPt=%d maxLog=%d accCatchupStart=%d, end=%d\n",
                                Arrays.toString(blockedSwapInAcceptors.toArray()), checkpointIndex,
                                maxLogIndex, acceptorCatchupStart, acceptorCatchupEnd);
                        //network.sendMessage(Configuration.clientIDs.get(1), new InfoMessage(myID, "startF"));
                        blockedSwapInAcceptors.clear();

                        if (!blockedSwapInLearners.isEmpty()) { // adjust active learners catchupSS timeCap
                            slackRatioAcc = 0.85;
                            for (int lea : learnerActiveSet.keySet()) {
                                network.sendMessage(new NodeIdentifier(lea), new SyncMessage(myID, -2, 0));
                            }
                            LOG.info("startF send sync<-2,0> => %s, slackRatioAcc=%.1f\n",
                                    learnerActiveSet.keySet(), slackRatioAcc);
                        }
                    }
                    ltoTimestamp = -1; // make sure execute this if(){} once
                    LOG.info("pReq enter ltoTimestamp=%d\n", ltoTimestamp);
                }

                if (!learnerSyncMsgBuf.isEmpty()) {
                    processPendingLearnerSync();
                }
                if (!acceptSyncMsgBuf.isEmpty()) {
                    processPendingAcceptorSync();
                }
            }

            if (enableBatching) {
                if (pendingReqCount >= BATCH_SIZE ||
                        System.currentTimeMillis() - lastBatchingTime >= BATCH_TIMEOUT) {
                    byte[] req;
                    List<byte[]> requests = new ArrayList<>();
                    int bbLen = 0;
                    Set<Map.Entry<Long, RequestMessage>> entrySet;
                    int cid;
                    outerLoop:
                    for (int cidHash : futureRequests.keySet()) {
                        futureReq = futureRequests.get(cidHash);
                        entrySet = futureReq.entrySet();
                        long expectedNum = maxRequestID.get(cidHash) + 1;
                        LOG.debug("outerLoop cidHash=%d sz=%d expected=%d\n", cidHash, futureReq.size(),
                                expectedNum);

                        innerLoop:
                        for (Map.Entry<Long, RequestMessage> e : entrySet) {
                            if (e.getKey() == expectedNum) {
                                maxRequestID.put(cidHash, expectedNum);
                                pendingReqCount--;
                                req = e.getValue().getRequest();
                                cid = cidHash & 0x0FFF;
                                ClientRequest cr = new ClientRequest(cid, expectedNum, req);
                                requests.add(cr.toRequestByteArray());
                                bbLen += cr.toRequestByteArray().length;

                                expectedNum++; //move forward the futureRequests
                                LOG.debug("batch cidHash=%d cid=%d cReq=%s\n", cidHash, cid, cr);
                                if (requests.size() >= BATCH_SIZE) {
                                    LOG.debug("break outerLoop requests size=%d reach max\n", requests.size());
                                    break outerLoop;
                                }
                            } else {
                                LOG.debug("break innerLoop reqNum=%d expected=%d\n", e.getKey(), expectedNum);
                                break innerLoop;
                            }
                        }
                        futureReq.headMap(expectedNum).clear();
                        LOG.debug("batching pReq expected=%d pendingCnt=%d cleanup\n", expectedNum, pendingReqCount);
                    }

                    if (!requests.isEmpty()) {
                        int numReqToBatch = requests.size();
                        byte[] chunk = new byte[bbLen + 4 * numReqToBatch + 4];
                        ByteBuffer bb = ByteBuffer.wrap(chunk);
                        bb.putInt(numReqToBatch); // numReqToBatch
                        for (byte[] b : requests) {
                            bb.putInt(b.length);
                            bb.put(b);
                        }
                        currentIndex++;
                        modifyAcceptQuorumsEntry(currentIndex, true);
                        AcceptMessage bAccept = new AcceptMessage(myID, maxRound, currentIndex,
                                firstUnstableIndex, chunk);
                        LOG.debug("batch bbLen=%d requestsSz=%d chunkLen=%d pendingCnt=%d\n", bbLen, requests.size(),
                                chunk.length, pendingReqCount);

                        //long ts = System.nanoTime();
                        for (int k : acceptSet.keySet()) {
                            if (enableFastPath && k == myID.hashCode()) {
                                network.localSendMessage(bAccept);
                                LOG.debug("pReq local send %s => %s\n", bAccept, myID);
                                // try {
                                //     fAccQueue.put(bAccept);
                                //     LOG.debug("pReq put %s => fAccQueue k=%d\n", bAccept, k);
                                // } catch (InterruptedException e1) {
                                //     e1.printStackTrace();
                                // }
                            } else {
                                network.sendMessage(new NodeIdentifier(k), bAccept);
                                LOG.debug("pReq send %s => %s\n", bAccept, new NodeIdentifier(k));
                            }
                        }
                        //msgProfiler.incMsgTiming(Message.MSG_TYPE.ACCEPT, System.nanoTime() - ts);
                        lastBatchingTime = System.currentTimeMillis();
                    }
                }
            } else {
                if (requestNum == maxReceivedNum + 1) { //fresh new request
                    long expectedNum = requestNum;
                    Set<Map.Entry<Long, RequestMessage>> entrySet = futureReq.entrySet();
                    for (Map.Entry<Long, RequestMessage> e : entrySet) {
                        if (e.getKey() == expectedNum) {
                            reqCount.addExecuted(); //vdr profiler
                            maxRequestID.put(senderHash, expectedNum);

                            currentIndex++;
                            // init accept quorum for current index entry
                            modifyAcceptQuorumsEntry(currentIndex, true);

                            ClientRequest cr = new ClientRequest(senderID, expectedNum, e.getValue().getRequest());
                            byte[] cReq = cr.toRequestByteArray();
                            byte[] chunk = new byte[cReq.length + 4 + 4];
                            ByteBuffer bb = ByteBuffer.wrap(chunk);
                            bb.putInt(1); // numReqToBatch
                            bb.putInt(cReq.length);
                            bb.put(cReq);
                            AcceptMessage acceptMsg = new AcceptMessage(myID, maxRound, currentIndex,
                                    firstUnstableIndex, chunk);
                            LOG.debug("pReq wrap req=%s cReqLen=%d accept=%s\n", e.getValue().getRequest(),
                                    cReq.length, acceptMsg);
                            // AcceptMessage acceptMsg = new AcceptMessage(myID, senderID, expectedNum, maxRound,
                            //         currentIndex, firstUnstableIndex, 1, e.getValue().getRequest());

                            //long ts = System.nanoTime();
                            for (int k : acceptSet.keySet()) {
                                if (enableFastPath && k == myID.hashCode()) {
                                    network.localSendMessage(acceptMsg);
                                    // try {
                                    //     fAccQueue.put(acceptMsg);
                                    // } catch (InterruptedException e1) {
                                    //     e1.printStackTrace();
                                    // }
                                    //if (!switchAcceptorDone)
                                    //    LOG.debug("add k=%d FastPath acc=%s\n", k, acceptSet.keySet());

                                } else {
                                    network.sendMessage(new NodeIdentifier(k), acceptMsg);
                                    if (!switchAcceptorDone)
                                        LOG.debug("pReq send %s => %s\n", acceptMsg, new NodeIdentifier(k));
                                }
                            }
                            //msgProfiler.incMsgTiming(Message.MSG_TYPE.ACCEPT, System.nanoTime() - ts);

                            expectedNum++; //move forward the futureRequests
                        } else {
                            if (!switchAcceptorDone)
                                LOG.debug("pReq break=>futureReq=%d expectedReq=%d\n", e.getKey(), expectedNum);
                            break;
                        }
                    }
                    futureReq.headMap(expectedNum).clear();
                    LOG.debug("pReq requestNum=%d, expectedNum=%d cleanup\n", requestNum, expectedNum);
                }
                // msgProfiler.incFuncTiming(MessageProfiler.FUNC_STAGE.S2, (System.nanoTime() - reqTs));
                // reqTs = System.nanoTime();
                // msgProfiler.incFuncTiming(MessageProfiler.FUNC_STAGE.S4, (System.nanoTime() - reqTs));
            }

            // msgProfiler.incFuncTiming(MessageProfiler.FUNC_STAGE.S3, (System.nanoTime() - reqTs));
            // reqTs = System.nanoTime();

        } else if (requestNum > maxAcceptedNum) {
            //leader created the quorum and got some accepted messages,
            //but not reach agreement, simply drop and wait for p2a timeout
            reqCount.addDropped(); //vdr profiler
            LOG.debug("pReq drop request=%d maxReceived=%d maxAccepted=%d wait for p2a timeout\n",
                    requestNum, maxReceivedNum, maxAcceptedNum);

        } else if (requestNum <= maxAcceptedNum) {
            // the client request already processed
            reqCount.addProcessed(); //vdr profiler
            LOG.debug("pReq already accepted request=%d < maxAccepted=%d\n", requestNum, maxAcceptedNum);

            //set request[]=>null, might not needed
            //set (index => 0) to indicate the processed request, and it is
            //hard for acceptor to decide the index for the processed request
            //DecisionMessage dec = new DecisionMessage(myID, senderID, requestNum, 0, null);
            ClientRequest cr = new ClientRequest(senderID, requestNum, null);
            byte[] cReq = cr.toRequestByteArray();
            byte[] chunk = new byte[cReq.length + 4 + 4];
            ByteBuffer bb = ByteBuffer.wrap(chunk);
            bb.putInt(1); // numReq
            bb.putInt(cReq.length);
            bb.put(cReq);
            DecisionMessage dec = new DecisionMessage(myID, 0, chunk);

            if (learnerActiveSet.isEmpty()) {
                LOG.error("error resend learnerActiveSet is empty\n");
                return;
            }
            for (int i : learnerActiveSet.keySet()) {
                NodeIdentifier lea = new NodeIdentifier(i);
                network.sendMessage(lea, dec);
            }
            if (!switchAcceptorDone)
                LOG.debug("pReq resend %s => %s\n", dec, learnerActiveSet.keySet());
        }
    }

    private void processAccept(AcceptMessage msg, int batchMode) {

        // int clientID = msg.getClientIDHash();
        long logIndex = msg.getIndex();
        int msgRound = msg.getRound();

        pingAcceptorTracks.put(msg.getSender().hashCode(), System.currentTimeMillis());

        if (msgRound < maxRound) {
            //current processAccepted handle this, or we can send acceptedMessage as a preempt message
            LOG.debug("pAcc processAccept drop %s msgRound=%d < maxRound=%d\n", msg, msg.getRound(), maxRound);
            return;
        } else if (msgRound > maxRound) {
            //change leader and maxRound in case new leader overwrite same logIndex
            maxRound = msgRound;

            if (isLeader && !leaderID.equals(msg.getSender())) {
                isLeader = false;
            }
            leaderID = msg.getSender();
            LOG.warning("pAcc change leaderID=%s maxRound=%d\n", leaderID, maxRound);
        }

        acceptorLog.update(msg);
        //acceptorLog.sync(); //disable

        acceptorLogCache.put(logIndex, msg);

        maxLogIndex = Math.max(maxLogIndex, logIndex);

        // //todo batching move this logic to pReq() for proposer only
        // if (msg.getClientReqNum() > 0) { //skip "NO-OP"
        //     int clientHash = (new NodeIdentifier(NodeIdentifier.Role.CLIENT, clientID)).hashCode();
        //     //long prev = (maxRequestID.get(clientHash) == null) ? 0 : maxRequestID.get(clientHash);
        //     long prev = maxRequestID.get(clientHash);
        //     maxRequestID.put(clientHash, Math.max(msg.getClientReqNum(), prev));
        // }

        //update firstUnstableIndex on general acceptors
        if (!myID.equals(msg.getSender())) {
            long catchupIdx = msg.getFirstUnstable();
            while (firstUnstableIndex < catchupIdx) {
                if (acceptorLogCache.get(firstUnstableIndex) != null &&
                        (acceptorLogCache.get(firstUnstableIndex).getIndex() >= firstUnstableIndex)) {
                    //change from "==" to ">="
                    firstUnstableIndex++;
                } else {
                    //LOG.info("update firstUnstableIndex %d, catchupIdx %d, logIndex %d\n",
                    //        firstUnstableIndex, catchupIdx, logIndex);
                    break;
                }
            }
        }

        //todo: possible bug for fresh new leader catchup, follower's firstUn is advanced
        if (!isLeader && /*!acceptorCatchupDone && */ logIndex < firstUnstableIndex) {
            LOG.debug("pAcc XXX isLeader %b, acceptorCatchupDone %b, logIndex %d, Unstable %d\n",
                    isLeader, acceptorCatchupDone, logIndex, firstUnstableIndex);
        } else {
            // if (batchMode == 0) {
            //     AcceptedMessage acceptedMsg =
            //             new AcceptedMessage(myID, msg.getRound(), logIndex);
            //     network.sendMessage(msg.getSender(), acceptedMsg);
            //     LOG.debug("send p2b %s => %s\n", acceptedMsg, msg.getSender());
            // } else
            if (batchMode == 1) {
                if (batchAcceptedStartIndex == 0) {
                    batchAcceptedStartIndex = logIndex;
                    LOG.info("pAcc #batch init batchAcceptedStartIndex=%d\n", batchAcceptedStartIndex);
                }
            } else if (batchMode == 2) {
                if (batchAcceptedStartIndex > 0 && logIndex > batchAcceptedStartIndex) {
                    BatchAcceptedMessage ba = new BatchAcceptedMessage(myID, msg.getRound(),
                            batchAcceptedStartIndex, logIndex);
                    network.sendMessage(msg.getSender(), ba);
                    batchAcceptedStartIndex = 0;
                    LOG.info("pAcc send p2b batch %s => %s\n", ba, msg.getSender());
                } else {
                    LOG.error("pAcc #error batch batchAcceptedStartIndex=%d, logIndex=%d\n",
                            batchAcceptedStartIndex, logIndex);
                }
            } else { //default=0
                AcceptedMessage acceptedMsg = new AcceptedMessage(myID, msgRound, logIndex);
                //long ts = System.nanoTime();
                if (enableFastPath && myID.equals(msg.getSender())) {
                    network.localSendMessage(acceptedMsg);
                    LOG.debug("pAcc local send %s => %s\n", acceptedMsg, myID);
                    // try {
                    //     fAccdQueue.put(acceptedMsg);
                    // } catch (InterruptedException e) {
                    //     e.printStackTrace();
                    // }
                } else {
                    network.sendMessage(msg.getSender(), acceptedMsg);
                    if (!switchAcceptorDone)
                        LOG.info("pAcc send p2b %s => %s\n", acceptedMsg, msg.getSender());
                }
                //msgProfiler.incMsgTiming(Message.MSG_TYPE.ACCEPTED, System.nanoTime() - ts);
                // if (0 == logIndex % SHOW_INTERVAL) {
                //     msgProfiler.setProgramTime(System.currentTimeMillis());
                //     msgProfiler.showMsgTiming();
                // }
            }
        }

        // NEED NOT disable cleanup LogCache during rePropose as firstUn being reset to previous index
        if (/*reProposeDone &&*/ firstUnstableIndex - lastLogCacheCleanIndex >
            maxAccLogCacheSize + Configuration.maxPendingRequests) {
            lastLogCacheCleanIndex += maxAccLogCacheSize;
            acceptorLogCache.headMap(lastLogCacheCleanIndex+1).clear();
            LOG.info("pAcc cleanupA acceptorLogCache lastLogCacheClean=%d, rPLastClean=%d, cacheSize=%d\n",
                    lastLogCacheCleanIndex, reProposeLastCacheCleanIndex, acceptorLogCache.size());
        }

    }

    private void leaderProcessAccepted(AcceptedMessage msg) {

        int senderHash = msg.getSender().hashCode();
        long index = msg.getIndex();
        int msgRound = msg.getRound();
        if (!switchAcceptorDone)
            LOG.debug("pAD receive %s\n", msg);
        pingAcceptorTracks.put(senderHash, System.currentTimeMillis());

        if (msgRound < maxRound) {
            LOG.info("pAD error msgRound=%d < maxRound=%d index=%d\n", msgRound, maxRound, index);
            return;
        } else if (msgRound > maxRound) {
            LOG.debug("pAD msgRound=%d > maxRound=%d index=%d\n", msgRound, maxRound, index);
            maxRound = msgRound;
            //leaderSelected = false;
            //startLeaderElection();
            return;
        }

        Quorum accept_qe = acceptQuorums.get(index);

        if (accept_qe == null) {
            // possibly since backup acceptor reply accepted, then leader receive
            // slow acceptor reply after delete the quorum entry
            if (index < firstUnstableIndex) { //todo check vdr ignore firstUnstableIndex protect here
                LOG.debug("pAD already reach consensus for quorum index %d, unstable %d\n",
                        index, firstUnstableIndex);
            } else {
                LOG.debug("pAD ERROR: quorum entry not exist\n");
            }
        } else {
            QuorumEntry qe = new QuorumEntry(true, 0);
            accept_qe.qrmap.put(senderHash, qe);
            accept_qe.incQuorum();

            LOG.debug("pAD get entry=%d quorum=%d maxLog=%d reset=%s => %s\n", index,
                    accept_qe.getQuorum(), maxLogIndex, msg.getSender(), accept_qe.qrmap.get(senderHash));

            if (accept_qe.recvAll()) {
                AcceptMessage etr = acceptorLogCache.get(index);
                reqCount.addAccepted();

                ClientRequest cr;
                int ch;
                long cn, prevMax;

                ByteBuffer bb = ByteBuffer.wrap(etr.getRequest());
                int sz = bb.getInt();
                byte[] reqStr;
                for (int i = 0; i < sz; i++) {
                    reqStr = new byte[bb.getInt()];
                    bb.get(reqStr);
                    cr = new ClientRequest();
                    cr.readFromRequestByteArray(reqStr);

                    ch = cr.getClientID();
                    cn = cr.getReqnum();
                    if (ch != 0) {
                        prevMax = maxRequestID.get(ch);
                        maxRequestID.put(ch, Math.max(cn, prevMax));
                    }

                    if (cn > 0) { //skip "NO-OP"
                        int clientHash = (new NodeIdentifier(NodeIdentifier.Role.CLIENT, ch)).hashCode();
                        prevMax = maxRequestID.get(clientHash);
                        maxRequestID.put(clientHash, Math.max(cn, prevMax));
                    }
                }
                showClientReqInfo();  //default in debug mode

                // delete this quorum
                modifyAcceptQuorumsEntry(index, false);
                LOG.debug("pAD remove acceptQuorumsEntry=%d\n",
                        index);

                if (firstUnstableIndex == index)
                    firstUnstableIndex++;

                // msg arrives out of order, e.g. #4, #5 reach agreement before #3, and after update
                // firstUnstableIndex from 2->3, it need update to #6. The log #index will be deleted once
                // reach agreement acceptQuorums #4 and #5 deleted before #3 done
                while (firstUnstableIndex <= maxLogIndex) {
                    if (!acceptQuorums.containsKey(firstUnstableIndex) &&
                            (reProposeDone ||
                                    (!reProposeDone && firstUnstableIndex < reProposeLastCacheIndex))) {
                        //if (!acceptQuorums.containsKey(firstUnstableIndex)) {
                        firstUnstableIndex++;
                    } else {
                        break;
                    }
                    LOG.debug("pAD INC firstUnstableIndex=%d, maxLogIndex=%d\n", firstUnstableIndex, maxLogIndex);
                }

                if (!reProposeDone && firstUnstableIndex > catchupEnd) {
                    ltoTimestamp = System.currentTimeMillis();
                    reProposeDone = true;
                    accLogIterator = null;
                    currentIndex = firstUnstableIndex - 1; //todo check correctness
                    double tm = (System.currentTimeMillis() - tsSwitchNewLeaderStart) / 1000.0;
                    network.sendMessage(Configuration.clientIDs.get(1), new InfoMessage(myID,
                            "finish switch new leader time(s)=" + tm));
                    LOG.info("#finish rePropose curIdx=>%d (P=%.0f, M=%.0f, Rs=%.0f Rr=%.0f)s, switchLeader=%.1f\n",
                            currentIndex, tsPrefetch, tsMerge, tsReProposeS, tsReProposeR, tm);
                    LOG.info("#finish rePropose (s) Rup=%.0f, profileA=%.1f, profileB=%.1f, ltoTimestamp=%d\n",
                            tsReProposeUp, tsProfileA / 1000.0, tsProfileB / 1000.0, ltoTimestamp);

                    if (learnerActiveSet.size() < Configuration.numQuorumLearners) {
                        LOG.info("##activeSet=%s < quorumSize=%d inActiveSet=%s switch learner\n",
                                learnerActiveSet.keySet(), Configuration.numQuorumLearners,
                                learnerInActiveSet.keySet());
                        leaderSwitchLearner(0); //specific swapOutLearner parameter
                    }
                }

                if (!switchAcceptorDone)
                    LOG.debug("pAD accepted firstUn=%d maxLog=%d reProposeDone=%b current=%d\n",
                        firstUnstableIndex, maxLogIndex, reProposeDone, currentIndex);

                // DecisionMessage decision = new DecisionMessage(myID, etr.getClientIDHash(),
                //         etr.getClientReqNum(), index, etr.getRequest());
                DecisionMessage decision = new DecisionMessage(myID, index, etr.getRequest());

                if (learnerActiveSet.isEmpty()) {
                    locateLearner();
                    LOG.error("pAD error learnerActiveSet is empty\n");
                    return; //todo check vdr this will skip outLoop2B check
                }
                //long ts = System.nanoTime();
                for (int i : learnerActiveSet.keySet()) {
                    NodeIdentifier lea = new NodeIdentifier(i);
                    NodeIdentifier oAcc = new NodeIdentifier(NodeIdentifier.Role.DUMMY, 0); //fake oAcc
                    if (enableDecInParallel && lea.getID() != myID.getID()
                        /* && switchAcceptorDone && switchLearnerDone && learnerCatchupLogDone */
                        ) {
                        oAcc = new NodeIdentifier(NodeIdentifier.Role.ACCEPTOR, lea.getID());
                    }

                    if (acceptSet.containsKey(oAcc.hashCode())) {
                        network.sendMessage(oAcc, new DecisionMessage(myID, index, null));
                        //LOG.info("send fake decision to other acceptor=%s\n", oAcc);
                    } else {
                        network.sendMessage(lea, decision);
                    }
                }
                if (!switchAcceptorDone)
                    LOG.debug("pAD send %s => %s\n", decision, learnerActiveSet.keySet());
                //msgProfiler.incMsgTiming(Message.MSG_TYPE.DECISION, System.nanoTime() - ts);
            }

            //(2B) check reProposeDone and accept/p2a timeout
            outLoop2B:
            if (reProposeDone && acceptQuorums.get(firstUnstableIndex) != null) {
                Quorum qr = acceptQuorums.get(firstUnstableIndex);
                for (int k : qr.qrmap.keySet()) {
                    if (k == myID.hashCode())
                        continue;
                    QuorumEntry entry = qr.qrmap.get(k);
                    if (entry.getTimeStamp() != 0 && !entry.getDone() &&
                        System.currentTimeMillis() - entry.getTimeStamp() > Configuration.acceptTimeout) {
                        LOG.info("pAD 2B notice %s entry=%s timeout swap it, firstUn=%d\n",
                                (new NodeIdentifier(k)), entry, firstUnstableIndex);
                        leaderSwitchAcceptor(firstUnstableIndex, k);
                        break outLoop2B;
                    }
                }
            }
            LOG.debug("pAD finish 2B check index=%d\n", index);
        }
    }

    private void processFakeDecision(DecisionMessage msg) {
        //todo fakefake
        // consider lastLogCacheCleanIndex
        long logIndex = msg.getIndex();
        AcceptMessage etr = acceptorLogCache.get(logIndex);
        DecisionMessage decision = new DecisionMessage(myID, logIndex, etr.getRequest());
        NodeIdentifier cLea = new NodeIdentifier(NodeIdentifier.Role.LEARNER, myID.getID());
        network.sendMessage(cLea, decision);
        //LOG.info("send real %s to corresponding learner=%s\n", decision, cLea);
    }

    private class ProcessRequestThread extends Thread {
        private boolean isRunning = true;
        private LinkedBlockingQueue<RequestMessage> reqQueue = new LinkedBlockingQueue<RequestMessage>(1000);
        private long ts;
        private double takeTime = 0.0;
        private double processTime = 0.0;
        private double totalTakeTime = 0.0;
        private double totalProcessTime = 0.0;
        private long counter = 0L;

        public void addTask(RequestMessage request){
            try{
                reqQueue.put(request);
            }
            catch(InterruptedException e){
                LOG.error(e);
                System.exit(-1);
            }
        }

        public void terminate(){
            isRunning = false;
            this.interrupt();
        }

        public void run(){
            while(isRunning){
                try{
                    ts = System.nanoTime();
                    RequestMessage request = reqQueue.take();
                    takeTime += (System.nanoTime() - ts)/1000000.0;
                    ts = System.nanoTime();
                    leaderProcessClientRequest(request);
                    processTime += (System.nanoTime() - ts)/1000000.0;
                    counter++;
                    if (0 == counter % SHOW_INTERVAL) {
                        totalTakeTime += takeTime;
                        totalProcessTime += processTime;
                        LOG.info("pRequest counter=%d take/process=%.1f,%.1f ms, total T/P=%.0f,%.0f s\n", counter,
                                takeTime, processTime, totalTakeTime/1000.0, totalProcessTime/1000.0);
                        takeTime = 0.0;
                        processTime = 0.0;
                    }
                    // msgProfiler.incMsgTiming(Message.MSG_TYPE.REQ, System.nanoTime() - ts);
                } catch(InterruptedException e){
                    //e.printStackTrace();
                    LOG.warning("ProcessRequestThread get interrupted\n");
                }
            }
        }
    }

    private class ProcessAcceptThread extends Thread {
        private LinkedBlockingQueue<AcceptMessage> accQueue = new LinkedBlockingQueue<AcceptMessage>(1000);
        private boolean isRunning = true;
        private long ts;
        private double takeTime = 0.0;
        private double processTime = 0.0;
        private double totalTakeTime = 0.0;
        private double totalProcessTime = 0.0;
        private long counter = 0L;

        public void addTask(AcceptMessage accept){
            try{
                accQueue.put(accept);
            }
            catch(InterruptedException e){
                LOG.error(e);
                System.exit(-1);
            }
        }

        public void run(){
            while(isRunning){
                try{
                    ts = System.nanoTime();
                    AcceptMessage accept = accQueue.take();
                    takeTime += (System.nanoTime() - ts)/1000000.0;
                    ts = System.nanoTime();
                    processAccept(accept, 0);
                    processTime += (System.nanoTime() - ts)/1000000.0;
                    counter++;
                    if (0 == counter % SHOW_INTERVAL) {
                        totalTakeTime += takeTime;
                        totalProcessTime += processTime;
                        LOG.info("pAccept counter=%d take/process=%.1f,%.1f ms, total T/P=%.0f,%.0f s\n", counter,
                                takeTime, processTime, totalTakeTime/1000.0, totalProcessTime/1000.0);
                        takeTime = 0.0;
                        processTime = 0.0;
                    }
                    // msgProfiler.incMsgTiming(Message.MSG_TYPE.ACCEPT, System.nanoTime() - ts);
                } catch(InterruptedException e){
                    //e.printStackTrace();
                    LOG.warning("ProcessAcceptThread get interrupted\n");
                }
            }
        }
    }

    private class ProcessAcceptedThread extends Thread {
        private LinkedBlockingQueue<AcceptedMessage> accdQueue = new LinkedBlockingQueue<AcceptedMessage>(1000);
        private boolean isRunning = true;
        private long ts;
        private double takeTime = 0.0;
        private double processTime = 0.0;
        private double totalTakeTime = 0.0;
        private double totalProcessTime = 0.0;
        private long counter = 0L;

        public void addTask(AcceptedMessage accepted){
            try{
                accdQueue.put(accepted);
            }
            catch(InterruptedException e){
                LOG.error(e);
                System.exit(-1);
            }
        }

        public void run(){
            while(isRunning){
                try{
                    ts = System.nanoTime();
                    AcceptedMessage accepted = accdQueue.take();
                    takeTime += (System.nanoTime() - ts)/1000000.0;
                    ts = System.nanoTime();
                    leaderProcessAccepted(accepted);
                    processTime += (System.nanoTime() - ts)/1000000.0;
                    counter++;
                    if (0 == counter % SHOW_INTERVAL) {
                        totalTakeTime += takeTime;
                        totalProcessTime += processTime;
                        LOG.info("pAccepted counter=%d take/process=%.1f,%.1f ms, total T/P=%.0f,%.0f s\n", counter,
                                takeTime, processTime, totalTakeTime/1000.0, totalProcessTime/1000.0);
                        takeTime = 0.0;
                        processTime = 0.0;
                    }
                    // msgProfiler.incMsgTiming(Message.MSG_TYPE.ACCEPTED, System.nanoTime() - ts);
                } catch(InterruptedException e){
                    // e.printStackTrace();
                    LOG.warning("ProcessAcceptedThread get interrupted\n");
                }
            }
        }
    }

}
