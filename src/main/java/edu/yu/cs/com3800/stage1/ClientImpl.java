package edu.yu.cs.com3800.stage1;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.yu.cs.com3800.LoggingServer;

public class ClientImpl implements Client, LoggingServer{
	private static AtomicInteger instanceCounter = new AtomicInteger();
	private Logger logger;
	private int hostPort;
	private String hostName;
	private HttpClient client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .connectTimeout(Duration.ofSeconds(8))
            .build();
	HttpResponse<String> response;
	
	public ClientImpl(String hostName, int hostPort) throws MalformedURLException{
		this.logger = initializeLogging(this.getClass().getName() + "-to-host-" + hostName + "-at-port-"+hostPort+"-instance-"+instanceCounter.getAndIncrement());
		logger.fine("New Client to host ["+hostName+"] on port ["+hostPort+"]");
		this.hostName = hostName;
		this.hostPort = hostPort;
	}

	@Override
	public void sendCompileAndRunRequest(String src) throws IOException {
		if(src ==null) {
			throw new IllegalArgumentException("Source code cannot be null");
		}
		logger.finest("Client to host ["+hostName+"] on port ["+hostPort+"] with new request [\n" + src + "\n]");
		//String adjusted = HttpEncodingUtils.encode(src);
		String uri = "http://"+hostName+":"+hostPort+"/compileandrun";
		HttpRequest request = HttpRequest.newBuilder()
				.POST(BodyPublishers.ofString(src))
                .uri(URI.create(uri))
                .setHeader("Content-Type", "text/x-java-source")
                .build();
		
		try {
			logger.finest("sending message request\n"+request.toString());
			response = client.send(request, HttpResponse.BodyHandlers.ofString());
			logger.finest("Response is: "+response.toString()+"\nStatus: "+response.statusCode()+"\nBody: "+response.body());
			logger.finest("Sucessful send");
			logger.fine("Client to host ["+hostName+"] on port ["+hostPort+"] with new request [\n" + src + "\n]\n" + "Response is: "+response.toString()+"\nStatus: "+response.statusCode()+"\nBody: "+response.body());
		} catch (InterruptedException e) {
			logger.log(Level.SEVERE, null, e);
		} catch (Exception e) {
			logger.fine(e.toString());
			throw e;
		}
	}

	//bad way of setting up, what if the client is used more than once?
	@Override
	public Response getResponse() throws IOException {
		if(this.response == null) {
			logger.finest("requesting Response object. Returning null");
			return null;
		} else {
			logger.finest("requesting Response object. Returning " + (new Response(this.response.statusCode(), this.response.body()).toString()));
			return new Response(this.response.statusCode(), this.response.body());
		}
	}
}
