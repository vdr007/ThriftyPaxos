package glassPaxos.apps.hashtable;

import glassPaxos.Configuration;
import glassPaxos.client.Client;
import glassPaxos.client.TestEntry;
import glassPaxos.interfaces.AppClientCallback;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Random;

public class AppClient implements AppClientCallback {

    public AppClient() {
        this.randomGenerator = new Random();
    }

    private Random randomGenerator;

    private void createTestEntries() {
        String rootPath = "/glassPaxos/simpleTest";
        TestEntry.OP_TYPE op;

        for (int i=1; i <= 200; i++) {
            String tPath = (new File(rootPath, String.valueOf(i))).toString();

            /* pure write test */
            op = TestEntry.OP_TYPE.SETDATA;

            /* pure read test */
            /*
            op = TestEntry.OP_TYPE.GETDATA;
            hybrid set/get data
            if (i%2 == 0) {
            op = TestEntry.OP_TYPE.SETDATA;
            } else {
            op = TestEntry.OP_TYPE.GETDATA;
            }
            */

            TestEntry te = new TestEntry(tPath, null, op.ordinal());
            //testEntries.add(te);
        }
    }

	public static void main(String []args) throws Exception {
        int rank = Integer.parseInt(args[0]);
        String configFile = args[1];
        int valueSize = Integer.parseInt(args[2]);

        AppClient app = new AppClient();
        Client testClient = new Client();
        testClient.initEnv(rank, configFile);

        int numClients = Configuration.numClients;
        int numTestPaths = Configuration.numTestPaths;
        int range = numTestPaths/numClients;
        int keyOffset = (rank-1) * range;
        //System.out.printf("I am %d/%d client totalTestPath=%d vLen=%d range=%d keyOffset=%d\n", rank, numClients,
        //        numTestPaths, valueSize, range, keyOffset);


        String tVal;
        int index;
        while(true) {
        //for(int i=0;i<50;i++) {
        //    Thread.sleep(200);
            /* use random string of vLen (pure letters) */
            tVal = RandomStringUtils.random(valueSize, true, false);
            index = app.randomGenerator.nextInt(numTestPaths);         //all key range
            //index = keyOffset + app.randomGenerator.nextInt(range);  //partial key range
            //TestEntry test = app.testEntries.get(index);
            //test.setValue(tVal);

            //testClient.sendRequest(test.transformBytes(), null);

            byte[] value = tVal.getBytes();
            byte[] req = new byte[1 + 8 + value.length];
            ByteBuffer b = ByteBuffer.wrap(req);
            b.put((byte)1);
            b.putLong((long) index);
            b.put(value);
            //System.out.printf("AppClient send PUT(k=%d, vLen=%d) tValLen=%d\n", index, value.length, tVal.length());
            //b.flip();
            testClient.sendRequest(req, app);
        }
	}

    @Override
    public void requestComplete(byte[] req, boolean completed, byte[] reply) {
        ByteBuffer rb = ByteBuffer.wrap(reply);
        byte op = rb.get();
        if(op == (byte)1) { //operation succeed
            String str = "SUCCEED";
            //System.out.printf("set request complete %s\n", str);
        } else {
            //System.out.printf("WRONG request complete\n");
        }
    }

}
