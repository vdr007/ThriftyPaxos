package glassPaxos.apps.hashtable;

import glassPaxos.Configuration;
import glassPaxos.SimpleLogger;
import glassPaxos.interfaces.AppServerInterface;
import glassPaxos.interfaces.Request;
import glassPaxos.learner.Learner;
import glassPaxos.network.messages.RequestMessage;
import org.apache.commons.configuration.ConfigurationException;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleEchoServer implements AppServerInterface {

	private static SimpleLogger LOG = SimpleLogger.getLogger("SimpleEchoServer");

	public SimpleEchoServer(){}

	@Override
	public byte[] executeRequest(Request req) {
		LOG.debug("echo receive request[%d] %s\n", req.request.length,
				RequestMessage.bytesToString(req.request));
		byte []reply;
		reply = new byte[1];
		reply[0] = (byte)1; //get succeed

		return reply;
	}

	@Override
	public void takeSnapshot(long index) {
		LOG.debug("void takeSnapshot\n");
	}

	@Override
	public void loadSnapshot(long index) {
		LOG.debug("void loadSnapshot\n");
	}

	@Override
	public List<byte[]> getSnapshotTokens(long index) {
		LOG.debug("void getSnapshotTokens\n");
		return null;
	}

	@Override
	public long getLatestSnapshotIndex() {
		return 0;
	}

	@Override
	public byte[] getSnapshotChunk(byte[] token) {
		LOG.debug("void getSnapshotChunk\n");
		return null;
	}

	@Override
	public void storeSnapshotChunk(byte[] token, byte[] data) {
		LOG.debug("void storeSnapshotChunk\n");
	}
	
	public static void main(String []args) throws ConfigurationException {
		int rank = Integer.parseInt(args[0]);
		String config = args[1];
		//Configuration.initConfiguration(config);

		Learner learner = new Learner();
		learner.setApplicationServer(new SimpleEchoServer());
		learner.start(rank, config);

		LOG = SimpleLogger.getLogger("AppServerInterface");
	}
}
