package glassPaxos.apps.hashtable;

import glassPaxos.SimpleLogger;

public class SSThread extends Thread {
    private static final SimpleLogger LOG = SimpleLogger.getLogger("Learner");
    private UncaughtExceptionHandler uncaughtExceptionalHandler = new UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            handleException(t.getName(), e);
        }
    };

    public SSThread(String threadName) {
        super(threadName);
        setUncaughtExceptionHandler(uncaughtExceptionalHandler);
    }

    protected void handleException(String thName, Throwable e) {
        LOG.debug("Exception occurred from thread {}", thName, e);
    }
}
