package edu.yu.cs.com3800;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

public class Util {

	/**
	 * EDIT Reading from a TCP connection has a potential issue in that if the server on the other end simply dies without the socket being properly closed,
	 * no FIN packet will be send to mark in.available() to be -1. For that reason we put a timeout on the while loop which terminates after 30 seconds.
	 * If there is a timeout, readAllBytes will return an empty byte[]
	 * @param in
	 * @return
	 */
    public static byte[] readAllBytesFromNetwork(InputStream in)  {
    	long startTime = System.currentTimeMillis();
        try {
            while (in.available() == 0 && System.currentTimeMillis() - startTime < 30000) {
                try {
					Thread.sleep(500);
                }
                catch (InterruptedException e) {
                }
            }
        }
        catch(IOException e){}
        return readAllBytes(in);
    }

    public static byte[] readAllBytes(InputStream in) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int numberRead;
        byte[] data = new byte[40960];
        try {
            while (in.available() > 0 && (numberRead = in.read(data, 0, data.length)) != -1   ) {
                buffer.write(data, 0, numberRead);
            }
        }catch(IOException e){}
        return buffer.toByteArray();
    }

    public static Thread startAsDaemon(Thread run, String name) {
        Thread thread = new Thread(run, name);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    public static String getStackTrace(Exception e){
        ByteArrayOutputStream bas = new ByteArrayOutputStream();
        PrintStream myErr = new PrintStream(bas,true);
        e.printStackTrace(myErr);
        myErr.flush();
        myErr.close();
        return bas.toString();
    }
    
    public static Integer loggingIteration = -1;
}
