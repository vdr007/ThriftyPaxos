package glassPaxos.client;

import java.io.File;
import java.io.UnsupportedEncodingException;

public class TestEntry {

    public TestEntry() {}

    public enum OP_TYPE {
        SETDATA,
        GETDATA,
    }

    private String path;
    private String value;
    private int op;
    private String info; //can be extended in the future

    public TestEntry(String path, String value, int op) {
        this.path = path;
        this.value = value;
        this.op = op;
        this.info = null;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String newPath) {
        path = newPath;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String newValue) {
        value = newValue;
    }

    public int getOp() {
        return op;
    }

    public void setOp(int newOp) {
        op = newOp;
    }

    public OP_TYPE getOpvalue() {
        return OP_TYPE.values()[op];
    }

    public String getInfo() {
        return info;
    }

    public byte[] transformBytes() {
        StringBuilder sb = new StringBuilder();
        sb.append(path).append(":")
        .append(value).append(":")
        .append(String.valueOf(op));
        byte[] b = null;
        try {
            b = sb.toString().getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return b;
    }

    public static String transformString(byte[] bb) {
        String str = null;
        try {
            str = new String(bb, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return str;
    }

    public static TestEntry transformTestEntry(String str) {
        String ss[] = str.split(":");
        TestEntry entry =
                new TestEntry(ss[0], ss[1], Integer.parseInt(ss[2]));
        return entry;
    }

    public String toString(){
        return String.format("<path: %s, value %s, op %s>",
                path, value, getOpvalue());
    }

    //public static void main(String []args) throws Exception{
    //    String rootpath = "/glassPaxosTest";
    //    OP_TYPE op = TestEntry.OP_TYPE.SETDATA;
    //    int i = 1024;
    //    String tPath = (new File(rootpath, String.valueOf(i))).toString();
    //    String tVal = "hello world";
    //    TestEntry te = new TestEntry(tPath, tVal, op.ordinal());
    //   byte[] res = te.transformBytes();
    //   System.out.format("transform: %s\n", TestEntry.transformString(res));
    //}

}
