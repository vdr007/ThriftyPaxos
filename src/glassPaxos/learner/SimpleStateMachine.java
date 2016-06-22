package glassPaxos.learner;

import glassPaxos.SimpleLogger;
import glassPaxos.client.TestEntry;
import glassPaxos.network.messages.RequestMessage;

import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
//import java.io.Serializable;

/* simple state machine
 * currently only support setData and getData operations
 */
public class SimpleStateMachine {
    private String path;
    private static ConcurrentHashMap<String, String> hashStateMachine;
    private SimpleLogger LOG = SimpleLogger.getLogger("SimpleStateMachine");

    public SimpleStateMachine(String path) {
        this.path = path;
        hashStateMachine = new ConcurrentHashMap<>();
    }

    public byte[] readSnapshot(String path) {
        /*
        todo: sender side
        return type: either snapshot file or byte array
        or we can return the path to the snapshot,
        and learner reads the file and send it over the network
        */

        /* fetch the snapshot file with createdIdx <= untilIdx */
        LOG.debug("==> enter readSnapshot ...\n");
        byte[] ss = path.getBytes();
        return ss;
    }

    public void storeSnapshot(byte[] snapshot, String path) {
        /*
        todo: receiver side
        input parameter can be snapshot file or byte array
        also the Parent path is the storage location
        store the snapshot file onto disk
        */
        LOG.debug("==> enter storeSnapshot ...\n");
    }

    public boolean snapShotExist(long index) {
        ArrayList<String> ssFileList =
                new ArrayList<>(fetchSnapshotList(index));
        if (ssFileList.isEmpty()) {
            return false;
        } else {
            return true;
        }
    }

    public StateEntry serializeStateEntry(String raw, String md5) {
        StateEntry se = new StateEntry();
        se.setRawString(raw);
        se.setMd5String(md5);
        return se;
    }

    /* create the snapshot of current hashStateMachine */
    public void createSnapshot(long checkpoint) {
        LOG.debug("learner create snapshot at checkpoint %d\n",
                checkpoint);
        try {
            String filename = "snapshotSM." + checkpoint;
            String outPath = new File(path, filename).toString();
            File oFile = new File(outPath);
            FileOutputStream fos = new FileOutputStream(oFile);
            ObjectOutputStream oos = new ObjectOutputStream(fos);

            // todo: change ConcurrentHashMap to TreeMap<index, <String, String>>
            /* store the StateMachine in memory */
            Map<String, String> tmap = new ConcurrentHashMap<>(hashStateMachine);
            oos.writeObject(tmap);
            //hashStateMachine.clear();
            oos.close();
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /* fetch the snapshot list with checkpoint <= index */
    public ArrayList<String> fetchSnapshotList(long index) {
        ArrayList<String> fileList = new ArrayList<>();
        File[] snapShots = new File(path).listFiles();
        LOG.debug("learner fetch snapshot before index %d, file[] %d\n", 
                index, snapShots.length);
        for (int i=0; i<snapShots.length; i++) {
            String fName = snapShots[i].getName();
            long ssIndex = Long.parseLong(fName.substring(fName.indexOf(".")+1));
            LOG.debug("ssIndex %d, index %d, path %s\n",
                    ssIndex, index, snapShots[i].getPath());
            if (ssIndex <= index) {
                fileList.add(snapShots[i].getPath());
            }

        }
        return fileList;
    }

    /* read and print one snapshot */
    public void showSnapshot(String filePath) {
        Map<String, String> mapInfile = null;
        try {
            File iFile = new File(filePath);
            FileInputStream fis = new FileInputStream(iFile);
            ObjectInputStream ois = new ObjectInputStream(fis);
            mapInfile = (Map)ois.readObject();
            ois.close();
            fis.close();
        }catch(IOException e) {
            e.printStackTrace();
        }catch(ClassNotFoundException c) {
            System.out.println("Class not found\n");
            c.printStackTrace();
        }
        ConcurrentHashMap<String, String> conHMap =
                new ConcurrentHashMap<>(mapInfile);

        /* print snapshot */
        Set set = conHMap.entrySet();
        Iterator iterator = set.iterator();
        LOG.debug("Learner print snapshot stats: \n");
        while(iterator.hasNext()) {
            Map.Entry etr = (Map.Entry)iterator.next();
            LOG.debug("K: %s, V: %s\n", etr.getKey(), etr.getValue());
        }
    }

    /* update the state machine based on setData/getData operations */
    public String updateState(TestEntry testEntry) {
        TestEntry.OP_TYPE opType = testEntry.getOpvalue();
        String path = testEntry.getPath();
        String value = testEntry.getValue();
        String res;

        switch (opType) {
            case SETDATA:
                res = setData(path, value);
                break;
            case GETDATA:
                res = getData(value);
                break;
            default:
                System.out.println("unsupported operation\n");
                res = "ERROR";
                break;
        }
        return res;
    }

    public String setData(String path, String value) {
        hashStateMachine.put(path, value);
        return "SUCCESS";
    }

    public String setMD5Data(String input) {
//        LOG.debug("learner call setData\n");
        MessageDigest m= null;
        try {
            m = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        assert m != null;
        m.update(input.getBytes(), 0, input.length());
        BigInteger bInt = new BigInteger(1, m.digest());

        hashStateMachine.put(input, bInt.toString(16));
//        System.out.println("MD5: "+ bInt.toString(16));
        return "SUCCESS";
    }

    public String getData(String path) {
//        LOG.debug("learner call getData\n");
        String result = hashStateMachine.get(path);
        if (result == null) result = "NOT EXIST";
        return result;
    }
}
