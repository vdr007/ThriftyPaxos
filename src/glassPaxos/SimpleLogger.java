package glassPaxos;

import java.sql.Timestamp;

public class SimpleLogger {
	
	public static final int ERROR = 3;
	public static final int WARNING = 2;
	public static final int INFO = 1;
	public static final int DEBUG = 0;
	
	private String name =  null;
	private boolean isActive = true;
	private int level = ERROR;
	
	private SimpleLogger(String name){
		this.name = name;
		this.reset();
	}
	
	public void reset(){
		this.isActive = Configuration.isLoggerActive(name);
		if(isActive)
			this.level = Configuration.getLoggerLevel(name);
	}
	
	public void error(String format, Object... args){
		if(isActive && level <= SimpleLogger.ERROR){
			System.out.print(name+": ");
			System.out.format(format, args);
		}
	}
	
	public void error(Throwable e){
		if(isActive && level <= SimpleLogger.ERROR){
			e.printStackTrace(System.out);
		}
	}
	
	public void warning(String format, Object... args){
		if(isActive && level <= SimpleLogger.WARNING){
			System.out.print(name+": ");
			System.out.format(format, args);
		}
	}
	
	public void warning(Throwable e){
		if(isActive && level <= SimpleLogger.WARNING){
			e.printStackTrace(System.out);
		}
	}
	
	public void info(String format, Object... args){
		if(isActive && level <= SimpleLogger.INFO){
			System.out.print(name+": ");
			System.out.format(format, args);
		}
	}

	public void infoTS(String format, Object... args){
		if(isActive && level <= SimpleLogger.DEBUG){
			System.out.print("[" + (new Timestamp(System.currentTimeMillis())) + "] ");
			System.out.format(format, args);
		}
	}

	public void info(Throwable e){
		if(isActive && level <= SimpleLogger.INFO){
			e.printStackTrace(System.out);
		}
	}
	
	public void debug(String format, Object... args){
		if(isActive && level <= SimpleLogger.DEBUG){
			System.out.print(name+": ");
			System.out.format(format, args);
		}
	}
	
	public void debug(Throwable e){
		if(isActive && level <= SimpleLogger.DEBUG){
			e.printStackTrace(System.out);
		}
	}
	
	public static SimpleLogger getLogger(String name){
		return new SimpleLogger(name);
	}
	
	public static void main(String args[]) throws Exception{
		Configuration.addActiveLogger("test", SimpleLogger.ERROR);
		
		SimpleLogger logger = SimpleLogger.getLogger("test");
		logger.error("hahaha %d\n hehe", 3);
	}
	
}
