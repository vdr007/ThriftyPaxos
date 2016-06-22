package glassPaxos.rstest;

import glassPaxos.acceptor.Acceptor;
import glassPaxos.Configuration;
import glassPaxos.SimpleLogger;

public class PerformanceAcceptor {
    private static String hostname;
    private static String configfile_path;

	public static void main(String []args) throws Exception{
    	Configuration.addActiveLogger("NettyNetwork", SimpleLogger.INFO);
        hostname = args[0]; 
        String lc = hostname.substring(hostname.length()-1);
        int rank = Integer.parseInt(lc);
        //System.out.println("\nacceptor rank: " + rank + "\n");
        configfile_path = args[1];
        Configuration.initConfiguration(configfile_path);
		Acceptor acceptor = new Acceptor(Configuration.acceptorIDs.get(hostname));
	}
}	
