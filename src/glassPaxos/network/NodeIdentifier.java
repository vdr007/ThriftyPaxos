package glassPaxos.network;

/*
 * NodeIdentifier is a unique integer identifier for a node.
 * Its first 8 bits identifies the role of the node (Client, Acceptor, or Learner)
 * Its last 24 bits is the ID of the node.
 */
public class NodeIdentifier {
	public static enum Role {
		DUMMY, CLIENT, ACCEPTOR, LEARNER
	}
	
	private int hashCode;
	
	public NodeIdentifier(Role role, int id){
		if(id>(1<<24))
			throw new RuntimeException("id too large: "+id);
		hashCode = (role.ordinal() << 24) | id;
	}
	
	public NodeIdentifier(int hashCode){
		this.hashCode = hashCode;
	}
	
	public Role getRole(){
		return Role.values()[hashCode >> 24];
	}
	
	public int getID(){
		return hashCode & 0x0FFF;
	}
	
	@Override
	public int hashCode(){
		return hashCode;
	}
	
	@Override
	public boolean equals(Object o){
		NodeIdentifier n = (NodeIdentifier)o;
		return this.hashCode == n.hashCode;
	}
	
	@Override
	public String toString(){
		return getRole().toString() + "." + getID();
	}
	
	public static void main(String args[]) throws Exception{
		NodeIdentifier id = new NodeIdentifier(NodeIdentifier.Role.LEARNER, 500);
		System.out.println(id);
		System.out.println(id.hashCode());
	}
}
