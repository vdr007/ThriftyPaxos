package glassPaxos.storage;

import glassPaxos.Configuration;
import glassPaxos.SimpleLogger;

import java.io.*;

public class AcceptorGCIndex {
    //private SimpleLogger LOG = SimpleLogger.getLogger("AcceptorGCIndex");
    private final String path;

    public AcceptorGCIndex(String path) {
        this.path = path;
    }

    public static void writeGCIndex(String filename, long index) throws IOException {
        FileOutputStream fos = null;
        DataOutputStream dos = null;

        File f = new File(filename);
        f.createNewFile();
        try {
            fos = new FileOutputStream(f, false);  //overwrite
            dos = new DataOutputStream(fos);
            dos.writeLong(index);
            dos.flush();
            System.out.printf("write GCIndex %d\n", index);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (dos != null)
                dos.close();
            if (fos != null)
                fos.close();
        }
    }

    public static long readGCIndex(String filename) throws IOException {
        long idx = 0L;
        File f = new File(filename);
        if (f.exists()) {
            FileInputStream fis = new FileInputStream(filename);
            DataInputStream dis = new DataInputStream(fis);
            if (dis.available() > 0)
                idx = dis.readLong();
            System.out.format("get GC Index <= %d\n", idx);
            if (dis != null)
                dis.close();
            if (fis != null)
                fis.close();
        }
        return idx;
    }

    public static void main(String []args) throws Exception {
        Configuration.initConfiguration(args[0]);
        String name = Configuration.acceptorGCStatFile;
        //for (int i=1; i<10; i++)
        //    writeGCIndex(name, 432108000 + i * 10);

        long idx = readGCIndex(name);
        System.out.format("main read GCIndex %d\n", idx);
    }
}
