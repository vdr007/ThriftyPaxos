package glassPaxos;

import glassPaxos.network.NodeIdentifier;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

public class Configuration {

	/*
	 * This is usually the max number of nodes per role.
	 * For example, if you have 1000 clients, 5 acceptors, and
	 * 5 learners, then set this number to 1000.
	 */
	//public static final int MAX_MSG_SIZE = 1048576;
	public static final int MAX_MSG_SIZE = 5242880;
	public static final int MAX_NODE_ID = 1000;

	//public static final long MAX_SNAPSHOT_CHUNK_SIZE = 1000L;
	public static final long MAX_SNAPSHOT_CHUNK_SIZE = 5242880L; //5M

	public enum PROTOCOL_TYPE {
		THRIFTY, STD, CHEAP
	}

	public static PROTOCOL_TYPE protocolType;

	/* timeout in millisecond */
	public static long pingTimeout;
	public static long requestTimeout;
	public static long prepareTimeout;
	public static long acceptTimeout;
	public static long learnerTimeout;

	public static int showPerformanceInterval;
	public static int checkpointInterval;
	//public static int freeCacheInterval;
	public static int maxAccSyncDiff;            // acceptor sync accept chunkSize
	public static int maxSyncDiff;               // sync decision/accept chunkSize
	public static int maxPendingCatchupSync;     // max pending requests of decision/accept catchupSyncMsg
	public static int maxDecisionGap;            // decision gap threshold for learner to catchup leader
	public static int catchupSnapshotThrottle;   // threshold for learner handling pending catchupSnapshot
	public static int catchupDecisionThrottle;   // threshold for leader handling pending catchupDecision
	public static int catchupLeaLogThreshold;    // threshold for leader handling pending learnerLog decision
	public static int leaCatchupTimeCap;         // expected execution time of learner catchup procedure (s)

	public static int batchingSize;              //batching size (#request)
	public static boolean enableBatching;        //enable batching mode
	public static int maxLogCacheSize;           //max cache size for acceptorLog and pending decisions
	public static boolean enableDecInParallel;   //enable acceptors send decision in parallel
	public static boolean enableLeaderFastPath;  //enable leader put accept/accepted in event queue directly
	public static boolean enableFailDetection;   //enable handleFailure logic

	/* request */
	public static int maxOutgoingRequests;
	public static int maxPendingRequests;

	public static int numTestPaths;

	public static int numClients;
	public static int numLearners;
	public static int numAcceptors;
	public static int numQuorumAcceptors;
	public static int numQuorumLearners;

    public final static HashMap<Integer, NodeIdentifier> clientIDs = new HashMap<>(); //index [1,...numClients]
    public final static HashMap<Integer, NodeIdentifier> acceptorIDs = new HashMap<>();
    public final static HashMap<Integer, NodeIdentifier> learnerIDs = new HashMap<>();

	/* log and snapshot directories */
	public static String acceptorGCStatFile;       // store additional acceptor stats
	public static String acceptorLogDir;           // store acceptorMessage
	public static String catchupAccLogDir;         // store catchup acceptorMessage
	public static String learnerLogDir;            // store new decisionMsg during learner catchup
	public static String catchupLeaLogDir;         // store missing decisionMsg during learner catchup
	public static String learnerSnapshotDir;       // persistent snapshot files
	public static String learnerFakeSnapshotFile;  // store fake snapshot files

	public static int debugLevel;

	public static PropertiesConfiguration gpConf;

	/* Logger configuration */
	private static HashMap<String, Integer> activeLogger = new HashMap<String, Integer>();
	
	public static boolean isLoggerActive(String name){
		return activeLogger.containsKey(name);
	}
	
	public static int getLoggerLevel(String name){
		return activeLogger.get(name);
	}
	
	public static void addActiveLogger(String name, int level){
		activeLogger.put(name, level);
	}
	
	public static void removeActiveLogger(String name){
		activeLogger.remove(name);
	}
	
	/* Network configuration */
	private static HashMap<NodeIdentifier, InetSocketAddress> nodes =
			new HashMap<NodeIdentifier, InetSocketAddress>();
	
	public static InetSocketAddress getNodeAddress(NodeIdentifier node){
		return nodes.get(node);
	}
	
	public static void addNodeAddress(NodeIdentifier node, InetSocketAddress address){
		nodes.put(node, address);
	}

    public static void initConfiguration(String confFile) throws ConfigurationException {
        gpConf = new PropertiesConfiguration(confFile);

		configNodeAddress(NodeIdentifier.Role.CLIENT, "client", gpConf.getInt("clientPort"));
        configNodeAddress(NodeIdentifier.Role.ACCEPTOR, "acceptor", gpConf.getInt("acceptorPort"));
        configNodeAddress(NodeIdentifier.Role.LEARNER, "learner", gpConf.getInt("learnerPort"));
		numClients = clientIDs.size();
		numAcceptors = acceptorIDs.size();
		numLearners = learnerIDs.size();
		numQuorumAcceptors = numAcceptors/2 + 1;
		numQuorumLearners = numLearners/2 + 1;

		String pType = gpConf.getString("protocolType", "THRIFTY");
		protocolType = PROTOCOL_TYPE.valueOf(pType);

		acceptorGCStatFile = gpConf.getString("acceptorGCStatFile");

		acceptorLogDir = gpConf.getString("acceptorLogDir");
		catchupAccLogDir = gpConf.getString("catchupAccLogDir");
		learnerLogDir = gpConf.getString("learnerLogDir");
		catchupLeaLogDir = gpConf.getString("catchupLeaLogDir");
		learnerSnapshotDir = gpConf.getString("learnerSnapShotDir");
		learnerFakeSnapshotFile = gpConf.getString("learnerFakeSnapshotFile");

		/* requests */
		maxOutgoingRequests = gpConf.getInt("outgoingRequests", 500);
		maxPendingRequests = gpConf.getInt("pendingRequests", 200);

		/* timeout */
		pingTimeout = gpConf.getInt("pingTimeout", 200);
		requestTimeout = gpConf.getInt("requestTimeout", 2000);
		prepareTimeout = gpConf.getInt("prepareTimeout", 200);
		acceptTimeout = gpConf.getInt("acceptTimeout", 800);
		learnerTimeout = gpConf.getInt("learnerTimeout", 2000);

		/* log checkpoint */
		showPerformanceInterval = gpConf.getInt("showPerformanceInterval", 10);
		checkpointInterval = gpConf.getInt("checkpointInterval", 3000000);
		//freeCacheInterval = gpConf.getInt("freeCacheInterval", 2000);

		maxAccSyncDiff = gpConf.getInt("maxAccSyncDiff", 2000);
		maxSyncDiff = gpConf.getInt("maxSyncDiff", 2000);
		maxDecisionGap = gpConf.getInt("maxDecisionGap", 50);
		maxPendingCatchupSync = gpConf.getInt("maxPendingCatchupSync", 10);
		catchupSnapshotThrottle = gpConf.getInt("catchupSnapshotThrottle", 15000);
		catchupDecisionThrottle = gpConf.getInt("catchupDecisionThrottle", 15000);
		catchupLeaLogThreshold  = gpConf.getInt("catchupLeaLogThreshold", 10000);
		leaCatchupTimeCap = gpConf.getInt("leaCatchupTimeCap", 200);

		numTestPaths = gpConf.getInt("numTestPaths", 1024*1024);

		debugLevel = gpConf.getInt("debugLevel", 1); //default value is INFO

		batchingSize   = gpConf.getInt("batchingSize", 50);
		enableBatching = gpConf.getBoolean("enableBatching", false);
		if (!enableBatching) batchingSize = 1;

		maxLogCacheSize = gpConf.getInt("maxLogCacheSize", 400000); //default 400k
		enableDecInParallel = gpConf.getBoolean("enableDecInParallel", false);
		enableLeaderFastPath = gpConf.getBoolean("enableLeaderFastPath", false);
		enableFailDetection  = gpConf.getBoolean("enableFailDetection", true);
    }

	public static void changeDebugLevel(int value) {
		debugLevel = value;
	}

	public static void showNodeConfig() {
		System.out.format("\n== show node configuration ==\n");
		Set nodeSet = clientIDs.entrySet();
		System.out.format("%d clients: %s\n", numClients, nodeSet);
		nodeSet = acceptorIDs.entrySet();
		System.out.format("%d acceptors: %s\n", numAcceptors, nodeSet);
		nodeSet = learnerIDs.entrySet();
		System.out.format("%d learners: %s\n", numLearners, nodeSet);
		System.out.format("quorum %d acceptors, %d learners\n", numQuorumAcceptors, numQuorumLearners);
		System.out.format("use protocol %s batchingMode=%b cacheSz=%d\n", protocolType.name(),
			enableBatching, maxLogCacheSize);
	}

	public static void displayMemoryInfo(String idInfo) {
		System.out.printf("%s memory(mb) max %d free %d\n", idInfo,
				Runtime.getRuntime().maxMemory()/(1024*1024),
				Runtime.getRuntime().freeMemory()/(1024*1024));
	}

    public static void configNodeAddress(NodeIdentifier.Role role, String keys, int startPort) {
        Iterator<String> names = gpConf.getKeys(keys);
        int idx = 1;
		while (names.hasNext()) {
			String name = names.next();
            InetSocketAddress iAddress = new InetSocketAddress(gpConf.getString(name), startPort+idx);
			NodeIdentifier node = new NodeIdentifier(role, idx);
		    addNodeAddress(node, iAddress);
			//System.out.format("=> add NodeAddress <%s, %s>, IDs <%d, %s>\n", 
            //        node, nodes.get(node), idx, node);

			if (role == NodeIdentifier.Role.CLIENT) {
				clientIDs.put(idx, node);
			} else if (role == NodeIdentifier.Role.ACCEPTOR) {
				acceptorIDs.put(idx, node);
			} else {
				learnerIDs.put(idx, node);
			}
            idx++;
		}
        //return idx-1;
    }

}
