package glassPaxos.rstest;

import glassPaxos.learner.Learner;
import glassPaxos.Configuration;
import glassPaxos.SimpleLogger;

public class PerformanceLearner {
    private static String hostname;
    private static String configfile_path;

	public static void main(String []args) throws Exception{
		Configuration.addActiveLogger("NettyNetwork", SimpleLogger.INFO);
        hostname = args[0]; 
        String lc = hostname.substring(hostname.length()-1);
        int rank = Integer.parseInt(lc);
        configfile_path = args[1];
        Configuration.initConfiguration(configfile_path);
		Learner learner = new Learner();
	}
}
