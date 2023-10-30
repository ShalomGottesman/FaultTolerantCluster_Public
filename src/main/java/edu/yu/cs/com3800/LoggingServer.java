package edu.yu.cs.com3800;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public interface LoggingServer {
	public static final String LOGGING_PREFIX = "edu.yu.cs.com3800.LOGGING_PREFIX";
	public static final ConcurrentHashMap<Logger, String> log2fileLocation = new ConcurrentHashMap<>();
	default Logger initializeLogging(String name)  {
		try {
			File logFolder = new File(System.getProperty("user.dir")+File.separator+"logs");
			synchronized(Util.loggingIteration){
				if(Util.loggingIteration == -1) {
					if(!logFolder.exists()) {
						logFolder.mkdirs();
					}
					File[] directories = logFolder.listFiles(File::isDirectory);
					int maxIter = 0;
					for(File f : directories) {
						String dirName = f.getName();
						try {
							int iter = Integer.parseInt(dirName);
							if(iter > maxIter) {
								maxIter = iter;
							}
						} catch (Exception e) {}
					}
					Util.loggingIteration = maxIter+1;
				}
				
			}
			String prefix = System.getProperty(LOGGING_PREFIX);
			Logger l;
			if(prefix == null || prefix.length() == 0) {
				l = Logger.getLogger(name);
			} else {
				l = Logger.getLogger(prefix +"-"+ name);
			}
			FileHandler fh;
			File loggingDir = null;
			if(prefix == null || prefix.length() == 0) {
				loggingDir = new File(logFolder.toString() + File.separator + Util.loggingIteration + File.separator);
			} else {
				loggingDir = new File(logFolder.toString() + File.separator + Util.loggingIteration + File.separator + prefix + File.separator);
			}
			loggingDir.mkdir();
			String fileLocation = loggingDir.toString() + File.separator + name + ".log";
			fh = new FileHandler(fileLocation);
			log2fileLocation.put(l, fileLocation);
			l.addHandler(fh);
			l.setLevel(Level.ALL);
			return l;
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}
