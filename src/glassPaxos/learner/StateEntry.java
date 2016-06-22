package glassPaxos.learner;

import java.io.Serializable;

public class StateEntry implements Serializable {
    private String rawString;
    private String md5String;

    public String getRawString() {
        return rawString;
    }

    public void setRawString(String rawString) {
        this.rawString = rawString;
    }

    public String getMd5String() {
        return md5String;
    }

    public void setMd5String(String md5String) {
        this.md5String = md5String;
    }

    @Override
    public String toString() {
        return new StringBuilder("raw: ")
                .append(this.rawString)
                .append("md5: ")
                .append(this.md5String).toString();
    }
}
