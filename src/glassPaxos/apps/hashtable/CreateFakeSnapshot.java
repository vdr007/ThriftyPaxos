package glassPaxos.apps.hashtable;

import org.apache.commons.lang3.RandomStringUtils;
import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class CreateFakeSnapshot {

    private static final int VAL_LEN = 512;
    private static final int GB_LEN = 1024 * 1024 * 1024/VAL_LEN;  //1GB
    private final HashMap<Long, byte[]> cacheMap = new HashMap<Long, byte[]>();

    public void generateFakeSnapshot(String snapshotPath, int gbSize) {
        int entries = gbSize * GB_LEN;
        String ssFileName = snapshotPath + File.separator + "fakeSnapshot." + gbSize + "G";
        System.out.printf("Start generating snapshot %s, gbSize=%d, entries %d\n", ssFileName, gbSize, entries);

        try {
            FileOutputStream fos = new FileOutputStream(new File(ssFileName));
            DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(fos));
            long chunkOffset;

            dos.writeInt(entries);
            for (long i=1L; i<=entries; i++) {
                //<k: i, v: sVal>
                String sVal = RandomStringUtils.random(VAL_LEN, true, false);
                cacheMap.put(i, sVal.getBytes());
                if (0 == i % GB_LEN) {
                    chunkOffset = i/GB_LEN;
                    batchWriteSnapshotChunk(ssFileName, chunkOffset, dos);
                    cacheMap.clear();
                }
            }
            dos.flush();
            dos.close();
            fos.close();
        } catch (FileNotFoundException e) {
            System.out.printf("error get FileNotFoundException in genFakeSnapshot");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.printf("error get IOException in genFakeSnapshot");
            e.printStackTrace();
        }
    }

    private void batchWriteSnapshotChunk(String filename, long chunkOffset, DataOutputStream output) throws IOException {
        long tmA = System.nanoTime();
        long totalLen = 0L;
        byte[] val;
        for (Map.Entry<Long, byte[]> entry : cacheMap.entrySet()) {
            output.writeLong(entry.getKey());
            val = entry.getValue();
            output.writeInt(val.length);
            output.write(val);
            totalLen += (8 + 4 + val.length);
        }

        long tmB = System.nanoTime();
        double execTm = (tmB-tmA)/1000000000.0;
        double wBW = totalLen/(execTm * 1024 * 1024);
        System.out.printf("genFakeSnapshot %s, chunk.%d, time %.2f s, WRITE %.1f MB/s, totalLen %d GB, cacheMap entries %d M\n",
                filename, chunkOffset, wBW, execTm, totalLen/(GB_LEN*VAL_LEN), cacheMap.size()/(1024*1024));
    }

    public static void main(String []args) throws Exception {
        if (args.length != 2) {
            System.out.printf("please input correct parameters CreateFakeSnapshot snapshotPath gbSize\n");
            return;
        }
        String snapshotPath = args[0];
        int gbSize = Integer.parseInt(args[1]);

        String absolutePath = (new File(snapshotPath)).getAbsolutePath();
        String parentPath = absolutePath.substring(0, absolutePath.lastIndexOf(File.separator));
        System.out.printf("absolutePath %s, parentPath %s\n", absolutePath, parentPath);
        CreateFakeSnapshot createFakeSnapshot = new CreateFakeSnapshot();
        createFakeSnapshot.generateFakeSnapshot(parentPath, gbSize);
    }
}
