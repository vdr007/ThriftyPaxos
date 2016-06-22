package glassPaxos.apps.hashtable;

import glassPaxos.Configuration;
import glassPaxos.SimpleLogger;
import glassPaxos.interfaces.AppServerInterface;
import glassPaxos.interfaces.LibServerInterface;
import glassPaxos.interfaces.Request;
import glassPaxos.learner.Learner;
import org.apache.commons.configuration.ConfigurationException;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class HashTableServer implements AppServerInterface {

	private final HashMap<Long, byte[]> map = new HashMap<Long,byte[]>();
	private final HashMap<Long, byte[]> backupMap = new HashMap<Long, byte[]>();
	private final Object lock = new Object();
	private boolean useBackup = false;
	private long takenCheckpoint = 0L;

	private Thread takeSnapshotInProcess = null;
	private Thread loadSnapshotInProcess = null;
	private LibServerInterface libServerInterface;

	private final String snapshotPath;
	private String fakeSnapshotPath;
	private static SimpleLogger LOG;
	private String snapshotFileDir;

	public HashTableServer(String snapshotPath){
		this.snapshotPath = snapshotPath;
		this.snapshotFileDir = this.snapshotPath + File.separator + "snapshot.";
	}

	@Override
	public byte[] executeRequest(Request req) {
		//disable because of bytesToString method
		//LOG.debug("request length %d: %s\n", req.request.length, RequestMessage.bytesToString(req.request));

		byte []reply;
		ByteBuffer b = ByteBuffer.wrap(req.request);
		byte op = b.get();
		//String sVal = null;

		if(op == (byte)1) { //write a key/value pair
			long key = b.getLong();
			byte[] value = new byte[b.remaining()];
			b.get(value);
			//try {
			//	sVal = new String(value, "UTF-8");
			//} catch (UnsupportedEncodingException e) {
			//	e.printStackTrace();
			//}
			//map.put(key, value);
			put(key, value);
			LOG.debug("put key=%d, value=dummy\n", key);
			reply = new byte[1];
			reply[0] = (byte)1; //put succeed
			//req.server.sendReply(req, reply);

		} else{
			long key = b.getLong();
			LOG.debug("get key=%d\n", key);
			//byte[] ret = map.get(key);
			byte[] ret = get(key);

			if(ret == null){ //key does not exist
				LOG.debug("get key=%d ret=null\n", key);
				reply = new byte[1];
				reply[0] = (byte)0; //get failed
				//req.server.sendReply(req, reply);

			} else{	//key exists.
				reply = new byte[1 + ret.length];
				reply[0] = (byte)1; //get succeed
				System.arraycopy(ret, 0, reply, 1, ret.length);
				//req.server.sendReply(req, reply);
			}
		}
		return reply;
		
	}

	private void put(long k, byte[] v) {
		synchronized (lock) {
			if (useBackup)
				backupMap.put(k, v);
			else
				map.put(k, v);
		}
	}

	private byte[] get(long k) {
		synchronized (lock) {
			if (useBackup)
				return backupMap.get(k);
			else
				return map.get(k);
		}
	}

	public void syncMap() {
		long start = System.nanoTime();
		map.putAll(backupMap);
		backupMap.clear();
		long end = System.nanoTime();
		LOG.debug("syncMap time %.2f, mapSz %d, backupMapSz %d, useBackup %b\n",
				(end-start)/1000000.0, map.size(), backupMap.size(), useBackup);
	}

	@Override
	public void takeSnapshot(long index) {
		if (takeSnapshotInProcess != null && takeSnapshotInProcess.isAlive()) {
			LOG.debug("busy take previous snapshot, skip %d\n", index);
			////libServerInterface.takeSnapshotComplete(0); //use 0 to indicate busy state
		} else {
			if (index <= takenCheckpoint) {
				LOG.warning("already take snapshot %d\n", index);
			} else {
				takeSnapshotInProcess = new TakeSnapshotThread("TakeSnapshotThread", index);
				takeSnapshotInProcess.start();
			}
		}
	}

	// @Override
	// public void loadSyncSnapshot(long index) {
	// 	loadRealSnapshot(index);
	// }

	@Override
	public void loadSnapshot(long index) {
		if (loadSnapshotInProcess != null && loadSnapshotInProcess.isAlive()) {
			LOG.debug("busy load previous snapshot, skip %d\n", index);
		} else {
			loadSnapshotInProcess = new LoadSnapshotThread("LoadSnapshotThread", index);
			loadSnapshotInProcess.start();
		}
	}

	public void loadRealSnapshot(long index) {
		String snapshotFileName = snapshotFileDir + index;
		LOG.info("Start loading snapshot %s\n", snapshotFileName);
		long tmStart = System.nanoTime();
		try {
			FileInputStream fis = new FileInputStream(new File(snapshotFileName));
			DataInputStream dis = new DataInputStream(new BufferedInputStream(fis));
			map.clear();
			int size = dis.readInt();
			LOG.info("snapshot size =%d\n", size);
			for(int i=0; i<size; i++){
				long key = dis.readLong();
				int valSize = dis.readInt();
				byte[] val = new byte[valSize];
				dis.readFully(val);
				LOG.debug("load key=%d\n", key);
				map.put(key, val);
			}
			dis.close();
			LOG.info("finish loading snapshot %s time %.1f ms\n", snapshotFileName,
					(System.nanoTime()-tmStart)/1000000.0);
		} catch (FileNotFoundException e) {
			LOG.error(e);
		} catch (IOException e) {
			LOG.error(e);
		}
	}

	private void loadFakeSnapshot(String fakeSnapshotFileName) {
		LOG.info("start loading fake snapshot %s\n", fakeSnapshotFileName);
		long tmStart = System.nanoTime();
		HashMap<Long, byte[]> fakeMap = new HashMap<Long, byte[]>();
		final int MAP_MAX_SIZE = 256*1024; //256K, 128M with 0.5K value
		try {
			FileInputStream fis = new FileInputStream(new File(fakeSnapshotFileName));
			DataInputStream dis = new DataInputStream(new BufferedInputStream(fis));
			int size = dis.readInt();
			int round = size/MAP_MAX_SIZE;
			LOG.info("fake snapshot size =%d, round %d\n", size, round);
			byte[] val;
			long key;
			int valSize;
			for(int r=0; r<round; r++) {
				for (int i = 0; i < MAP_MAX_SIZE; i++) {
					key = dis.readLong();
					valSize = dis.readInt();
					val = new byte[valSize];
					dis.readFully(val);
					fakeMap.put(key, val);
				}
				fakeMap.clear();
				if (0 == r%8)
					LOG.info("finish fetch fakeMap round=%d\n", r);
			}
			fakeMap.clear();
			dis.close();
			LOG.info("finish loading fake snapshot %s time %.2f s\n", fakeSnapshotFileName,
					(System.nanoTime()-tmStart)/1000000000.0);
		} catch (FileNotFoundException e) {
			LOG.error(e);
		} catch (IOException e) {
			LOG.error(e);
		}

	}

	@Override
	public List<byte[]> getSnapshotTokens(long index) {
		String snapshotFileName = "snapshot." + index;
		String filename = snapshotPath + File.separator + snapshotFileName;
		File f = new File(filename);
		if(!f.exists()){
			LOG.warning("%s does not exist\n", filename);
			return null;
		} else{
			LinkedList<byte[]> ret = new LinkedList<byte[]>();
			long size = f.length();
			long current = 0;
			while(current < size){
				if(size - current > Configuration.MAX_SNAPSHOT_CHUNK_SIZE){
					SnapshotToken token = new SnapshotToken(snapshotFileName, current, Configuration.MAX_SNAPSHOT_CHUNK_SIZE);
					current += Configuration.MAX_SNAPSHOT_CHUNK_SIZE;
					ret.add(token.toByteArray());
				}
				else{
					SnapshotToken token = new SnapshotToken(snapshotFileName, current, size - current);
					current = size;
					ret.add(token.toByteArray());
				}
			}
			List<byte[]> fakeTokens;
			fakeTokens = getFakeSnapshotTokens(Configuration.learnerFakeSnapshotFile);
			if (fakeTokens != null) {
				ret.addAll(fakeTokens);
				LOG.info("combine all fake snapshot tokens size add %d=>%d\n", fakeTokens.size(), ret.size());
			} else {
				LOG.info("fake snapshotTokens is empty\n");
			}
			return ret;
		}
	}

	private List<byte[]> getFakeSnapshotTokens(String fakeSnapshotFileName) {
		String snapshotName = fakeSnapshotFileName.substring(fakeSnapshotFileName.lastIndexOf(File.separator) + 1);
		LOG.info("fake snapshot path %s, filename %s\n", fakeSnapshotFileName, snapshotName);
		File f = new File(fakeSnapshotFileName);
		if(!f.exists()){
			LOG.warning("%s does not exist\n", fakeSnapshotFileName);
			return null;
		}
		else{
			LinkedList<byte[]> ret = new LinkedList<byte[]>();
			long size = f.length();
			long current = 0;
			while(current < size){
				if(size - current > Configuration.MAX_SNAPSHOT_CHUNK_SIZE){
					SnapshotToken token = new SnapshotToken(snapshotName, current,
							Configuration.MAX_SNAPSHOT_CHUNK_SIZE);
					current += Configuration.MAX_SNAPSHOT_CHUNK_SIZE;
					ret.add(token.toByteArray());
				}
				else{
					SnapshotToken token = new SnapshotToken(snapshotName, current, size - current);
					current = size;
					ret.add(token.toByteArray());
				}
			}
			return ret;
		}
	}

	@Override
	public long getLatestSnapshotIndex() {
		long ts = System.nanoTime();
		File root = new File(snapshotPath);
		if (!root.exists()) {
			LOG.debug("root dir %s does not exist\n", snapshotPath);
			root.mkdirs();
		}
		String f;
		long latestIndex = 0L;
		for (File file : root.listFiles()) {
			f = file.getName();
			latestIndex = Math.max(latestIndex,
					Long.parseLong(f.substring(f.lastIndexOf(".")+1), 10));
		}
		LOG.debug("get latest snapshot.%d on disk tm %.1f ms\n", latestIndex, (System.nanoTime()-ts)/1000000.0);
		return latestIndex;
	}

	@Override
	public byte[] getSnapshotChunk(byte[] token) {
		SnapshotToken sToken = new SnapshotToken(token);
		CharSequence fakeSeq = "fake";
		String filename;
		if (sToken.filename.contains(fakeSeq)) {
			filename = fakeSnapshotPath + File.separator + sToken.filename;
			LOG.debug("start getting snapshot chunk %s\n", sToken);
		} else {
			filename = snapshotPath + File.separator + sToken.filename;
			LOG.debug("start getting fake snapshot chunk %s\n", sToken);
		}
		RandomAccessFile f;
		try {
			f = new RandomAccessFile(filename, "r");
			f.seek(sToken.offset);
			byte[] data = new byte[(int)sToken.length];
			f.readFully(data);
			f.close();
			LOG.debug("finish getting snapshot chunk %s\n", sToken);
			return data;
		} catch (FileNotFoundException e) {
			LOG.error(e);
			return null;
		} catch (IOException e) {
			LOG.error(e);
			return null;
		}
	}

	@Deprecated
	private byte[] getFakeSnapshotChunk(byte[] token) {
		SnapshotToken sToken = new SnapshotToken(token);
		String filename = fakeSnapshotPath + File.separator + sToken.filename;
		LOG.debug("start getting fake snapshot chunk %s\n", sToken);
		RandomAccessFile f;
		try {
			f = new RandomAccessFile(filename, "r");
			f.seek(sToken.offset);
			byte[] data = new byte[(int)sToken.length];
			f.readFully(data);
			f.close();
			LOG.debug("finish getting fake snapshot chunk %s\n", sToken);
			return data;
		} catch (FileNotFoundException e) {
			LOG.error(e);
			return null;
		} catch (IOException e) {
			LOG.error(e);
			return null;
		}
	}

	@Override
	public void storeSnapshotChunk(byte[] token, byte[] data) {
		SnapshotToken sToken = new SnapshotToken(token);
		CharSequence fakeSeq = "fake";
		String filename;
		if (sToken.filename.contains(fakeSeq)) {
			filename = fakeSnapshotPath + File.separator + sToken.filename;
			LOG.debug("start storing fake snapshot chunk %s\n", sToken);
		} else {
			filename = snapshotPath + File.separator + sToken.filename;
			LOG.debug("start storing snapshot chunk %s\n", sToken);
		}
		RandomAccessFile f;
		try {
			//long tm1 = System.nanoTime();
			f = new RandomAccessFile(filename, "rw");
			//long tm2 = System.nanoTime();
			f.seek(sToken.offset);
			f.write(data);
			//long tm3 = System.nanoTime();
			f.close();
			//long tm4 = System.nanoTime();
			//LOG.debug("finish storing snapshot chunk %s time(ms) [o %.1f, sw %.1f, c %.1f]\n",
			//		sToken, (tm2-tm1)/1000000.0, (tm3-tm2)/1000000.0, (tm4-tm3)/1000000.0);
		} catch (FileNotFoundException e) {
			LOG.error("get FileNotFoundException: \n" + e);
		} catch (IOException e) {
			LOG.error("get IOException: \n" + e);
		}

	}

	@Deprecated
	private void storeFakeSnapshotChunk(byte[] token, byte[] data) {
		SnapshotToken sToken = new SnapshotToken(token);
		//String filename = snapshotPath + File.separator + sToken.filename; //used in copyFakeSnapshotTest only
		String filename = fakeSnapshotPath + File.separator + sToken.filename;
		LOG.debug("start storing fake snapshot chunk %s\n", sToken);
		RandomAccessFile f;
		try {
			//long tm1 = System.nanoTime();
			f = new RandomAccessFile(filename, "rw");
			//long tm2 = System.nanoTime();
			f.seek(sToken.offset);
			f.write(data);
			//long tm3 = System.nanoTime();
			f.close();
			//long tm4 = System.nanoTime();
			//LOG.debug("finish storing snapshot chunk %s time(ms) [o %.1f, sw %.1f, c %.1f]\n",
			//		sToken, (tm2-tm1)/1000000.0, (tm3-tm2)/1000000.0, (tm4-tm3)/1000000.0);
		} catch (FileNotFoundException e) {
			LOG.error("get FileNotFoundException: \n" + e);
		} catch (IOException e) {
			LOG.error("get IOException: \n" + e);
		}

	}

	class LoadSnapshotThread extends Thread {
		private final SimpleLogger LOG = SimpleLogger.getLogger("Learner");
		public long ckPt;
		private UncaughtExceptionHandler uncaughtExceptionalHandler = new UncaughtExceptionHandler() {
			@Override
			public void uncaughtException(Thread t, Throwable e) {
				handleException(t.getName(), e);
			}
		};

		public LoadSnapshotThread(String threadName, long index) {
			super(threadName);
			this.ckPt = index;
			setUncaughtExceptionHandler(uncaughtExceptionalHandler);
		}

		protected void handleException(String thName, Throwable e) {
			LOG.warning("Exception occurred from thread {}\n", thName, e);
			e.printStackTrace();
		}

		public void run() {
			long tmA = System.nanoTime();
			synchronized (lock) {
				useBackup = true;
			}
			LOG.info("start loading snapshot, set useBackup=%b\n", useBackup);
			loadRealSnapshot(ckPt);
			////loadFakeSnapshot(Configuration.learnerFakeSnapshotFile);

			long tmB = System.nanoTime();
			synchronized (lock) {
				syncMap();
				useBackup = false;
			}
			long tmC = System.nanoTime();
			LOG.info("load snapshot time (load %.2f, sync %.2f), mapSz=%d, backupSz=%d\n",
					(tmB-tmA)/1000000.0, (tmC-tmB)/1000000.0, map.size(), backupMap.size());

			libServerInterface.loadSnapshotComplete(ckPt);
		}
	}

	class TakeSnapshotThread extends Thread {
		private final SimpleLogger LOG = SimpleLogger.getLogger("Learner");
		public long ckPt;
		private UncaughtExceptionHandler uncaughtExceptionalHandler = new UncaughtExceptionHandler() {
			@Override
			public void uncaughtException(Thread t, Throwable e) {
				handleException(t.getName(), e);
			}
		};

		public TakeSnapshotThread(String threadName, long index) {
			super(threadName);
			this.ckPt = index;
			setUncaughtExceptionHandler(uncaughtExceptionalHandler);
		}

		protected void handleException(String thName, Throwable e) {
			LOG.warning("Exception occurred from thread {}\n", thName, e);
			e.printStackTrace();
		}

		public void run() {
			String ssFileName = snapshotPath + File.separator + "snapshot." + ckPt;
			long tmA = System.nanoTime();
			LOG.info("Start taking snapshot %s, mapSize =%d, useBackup %b\n", ssFileName, map.size(), useBackup);
			synchronized (lock) {
				useBackup = true;
			}

			long totalLen = 0L;

			try {
				FileOutputStream fos = new FileOutputStream(new File(ssFileName));
				DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(fos));
				dos.writeInt(map.size());
				byte[] val;
				//for (Long key : map.keySet()) {
				//	dos.writeLong(key);
				//	val = map.get(key);
				//	dos.writeInt(val.length);
				//	dos.write(val);
				//	totalLen += (8+4+val.length);
				//}

				for (Map.Entry<Long, byte[]> entry : map.entrySet()) {
					dos.writeLong(entry.getKey());
					val = entry.getValue();
					dos.writeInt(val.length);
					dos.write(val);
					totalLen += (8 + 4 + val.length);
				}
				dos.flush();
				dos.close();
				fos.close();
			} catch (FileNotFoundException e) {
				LOG.error("get FileNotFoundException in takeSnapshot");
				e.printStackTrace();
			} catch (IOException e) {
				LOG.error("get IOException in takeSnapshot");
				e.printStackTrace();
			}

			long tmB = System.nanoTime();
			synchronized (lock) {
				syncMap();
				useBackup = false;
				takenCheckpoint = Math.max(takenCheckpoint, ckPt);
			}
			long tmC = System.nanoTime();
			LOG.info("snapshot %s, time takeSS(s) %.1f, sync %.3f, mapSz=%d, backupSz=%d, totalLen %d MB\n",
					ssFileName, (tmB-tmA)/1000000000.0, (tmC-tmB)/1000000000.0,
					map.size(), backupMap.size(), totalLen/(1024*1024));

			libServerInterface.takeSnapshotComplete(ckPt);
		}
	}

	private static class SnapshotToken {
		public String filename;
		public long offset;
		public long length;

		public SnapshotToken(String filename, long offset, long length){
			this.filename = filename;
			this.offset = offset;
			this.length = length;
		}

		public SnapshotToken(byte[] bytes){
			this.readFromByteArray(bytes);
		}

		public byte[] toByteArray(){
			byte[] str = filename.getBytes();
			byte[] ret = new byte[4 + str.length + 8 + 8];
			ByteBuffer b = ByteBuffer.wrap(ret);
			b.putInt(str.length);
			b.put(str);
			b.putLong(offset);
			b.putLong(length);
			return ret;
		}

		public void readFromByteArray(byte[] bytes){
			ByteBuffer b = ByteBuffer.wrap(bytes);
			byte[] str = new byte[b.getInt()];
			b.get(str);
			filename = new String(str);
			offset = b.getLong();
			length = b.getLong();
		}

		@Override
		public String toString(){
			StringBuilder sb = new StringBuilder();
			sb.append(filename).append(" offset ").append(offset).append(" len ").append(length);
			return sb.toString();
		}

	}

	private void setLibServerInterface(LibServerInterface libServer) {
		libServerInterface = libServer;
	}

	private void copyFakeSnapshotTest(String fakeSS) {
		LOG.info("test fakeSS %s\n", fakeSS);
		loadFakeSnapshot(fakeSS);
		List<byte[]> tokens = getFakeSnapshotTokens(fakeSS);
		LOG.info("fetch tokens of fake snapshot %s size %d\n", fakeSS, tokens.size());
		for(byte[] token : tokens){
			storeFakeSnapshotChunk(token, getFakeSnapshotChunk(token));
			LOG.info("store chunk with token %s\n", new SnapshotToken(token));
		}
	}

	public static void main(String []args) throws ConfigurationException {
		int rank = Integer.parseInt(args[0]);
		String config = args[1];
		Configuration.initConfiguration(config);
		String snapshotDir = Configuration.learnerSnapshotDir;
		String fakeSS = Configuration.learnerFakeSnapshotFile;
		Configuration.addActiveLogger("AppServerInterface", Configuration.debugLevel);
		LOG = SimpleLogger.getLogger("AppServerInterface");

		Learner learner = new Learner();
		HashTableServer hashTableServer = new HashTableServer(snapshotDir);
		//hashTableServer.init();
		//String snapshotDir = Configuration.learnerSnapshotDir + "/learner" + rank;

		//learner.setApplicationServer(new HashTableServer(snapshotDir));
		hashTableServer.setLibServerInterface(learner);
		learner.setApplicationServer(hashTableServer);
		learner.start(rank, config);

		hashTableServer.fakeSnapshotPath = fakeSS.substring(0, fakeSS.lastIndexOf(File.separator));
		LOG.info("start HashTableServer with snapshotDir %s, fakeSnapshotDir %s\n", snapshotDir, hashTableServer.fakeSnapshotPath);

		//if (1 == rank) {
		//	hashTableServer.copyFakeSnapshotTest(fakeSS);
		//}
	}
}
