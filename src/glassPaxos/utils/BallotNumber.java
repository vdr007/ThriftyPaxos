package glassPaxos.utils;

import glassPaxos.network.NodeIdentifier;
//import io.netty.buffer.ByteBuf;
//import java.nio.ByteBuffer;

public class BallotNumber {

    private int hashCode;
  
    public BallotNumber(NodeIdentifier id, int round){
		if(round>(1<<24))
			throw new RuntimeException("round too large: "+round);
        if (id != null) {
            this.hashCode = (id.getID() << 24) | round;
        } else {
            this.hashCode = round;
        }
    }
  
    public BallotNumber(int hashCode){
        this.hashCode = hashCode;
    }

    public int getRound() {
        return hashCode & 0x0FFF;
    }

    public NodeIdentifier getID() {
        return new NodeIdentifier(NodeIdentifier.Role.ACCEPTOR, hashCode>>24);
    }

    public int hashCode() {
        return hashCode;
    }

    public boolean equals(Object other){
        return compareTo(other) == 0;
    }
  
    //only consider the round number
    public int compareTo(Object o){
        BallotNumber bn = (BallotNumber)o;
        return this.getRound() - bn.getRound();
        //if (bn.getRound() != this.getRound()) {
        //  return this.getRound() - bn.getRound();
        //}
        //return this.getAccid().getID() - bn.getAccid().getID();
    }
  
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("BN<id ").append(getID()).append(", round ").append(getRound()).append(">");
        return sb.toString();
    }
}
