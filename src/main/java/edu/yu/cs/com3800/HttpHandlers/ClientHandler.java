package edu.yu.cs.com3800.HttpHandlers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.stage5.GatewayPeerServerImpl;

public class ClientHandler implements HttpHandler, LoggingServer{
	static AtomicInteger instanceCounter = new AtomicInteger();
	Logger logger;
	GatewayPeerServerImpl gateway;
	ThreadPoolExecutor tpe = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 2, Runtime.getRuntime().availableProcessors() * 2, 100L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
	long requestCounter = 1;
	HashMap<Long, HttpExchange> activeExchangesByID = new HashMap<>();
	Queue<HttpExchange> queuedExchanges = new LinkedList<>();
	
	public ClientHandler (GatewayPeerServerImpl gatewayServer) {
		this.gateway = gatewayServer;
		this.logger = this.initializeLogging(this.getClass().getSimpleName()+"-instance-"+instanceCounter.getAndIncrement());
	}
	
	@Override
	public void handle(HttpExchange exchange) throws IOException {
		logger.fine(exchange.toString());
		try {
			switch(exchange.getRequestMethod()) {
			case "GET":
			case "POST":
				if(exchange.getRequestHeaders().get("Content-Type") != null && exchange.getRequestHeaders().get("Content-Type").contains("text/x-java-source")) {
					logger.fine("Request had proper header and method for processing");
					tpe.execute(new RunnableExchange(tpe, gateway, exchange, requestCounter++));
				} else {
					logger.fine("HTTP request did nto contain proper headers for Java Compilation, return a 400");
					exchange.sendResponseHeaders(400,  -1);
					exchange.close();
				}
				break;
			default:
				logger.fine("Request was not of type GET or POST, return 405");
				exchange.sendResponseHeaders(405, -1);
				exchange.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.fine(e.toString());
		}
	}
	
	private class RunnableExchange implements Runnable, LoggingServer{
		GatewayPeerServerImpl gateway;
		HttpExchange exchange;
		Logger logger;
		long requestID;
		int retryCount = 0;
		ThreadPoolExecutor tpe;
		RequestData data;
		
		public RunnableExchange(ThreadPoolExecutor tpe, GatewayPeerServerImpl gateway, HttpExchange exchange, long requestID) {
			this.logger = initializeLogging(this.getClass().getSimpleName()+ 
					"-servicing-exchange-"+requestID+"-from-"+exchange.getRemoteAddress().getHostName()
					+"-port["+exchange.getRemoteAddress().getPort()+"]");
			this.requestID = requestID;
			this.gateway = gateway;
			this.exchange = exchange;
			this.tpe = tpe;
			logger.finest("new RunnableExchange instance with request id "+requestID+" and exchange string "+exchange.toString());
		}
		
		private RunnableExchange(ThreadPoolExecutor tpe, GatewayPeerServerImpl gateway, HttpExchange exchange, long requestID, Logger logger, int retryCount, RequestData data) {
			this.logger = logger;
			this.gateway = gateway;
			this.exchange = exchange;
			this.requestID = requestID;
			this.retryCount = retryCount;
			this.data = data;
			this.tpe = tpe;
			logger.finest("continued RunnableExchange instance with request id "+requestID+" and exchange string "+exchange.toString()+". This is the "+retryCount+" retry");
		}
		@Override
		public void run() {
			if(data == null) {
				data = processRequest(exchange);
			}
			if(data == null) {
				logger.severe("Error reading in request from client. This is non recoverable.");
				return;
			}
			logger.fine("code in is\n"+data.code);
			//Compose Message object for internal transfer
			Message msg = new Message(MessageType.WORK, data.bytes, data.sourceHost, data.sourcePort, data.receivingHost, data.receivingPort, requestID);
			logger.fine("Compiled Message for internal use is:\n"+msg.toString());
			while(this.gateway.getLeaderAddress() == null) {
				synchronized(this.gateway.getLeaderMonitor()) {
					while(this.gateway.getLeaderAddress() == null) {
						try {
							this.gateway.getLeaderMonitor().wait();
							//when the gateway server finds the leader, this should be notified
						} catch (InterruptedException e) {
						}
					}
				}
			}
			InetSocketAddress leaderSockAddr = this.gateway.getLeaderAddress();
			logger.fine("retreived leader is at address\n"+leaderSockAddr.toString());
			Message responseFromLeader = null;
			try {
				Socket client2server = new Socket(InetAddress.getByName(this.gateway.getLeaderAddress().getHostName())
						, this.gateway.getLeaderAddress().getPort() + 2); //plus 2 for tcp port
				logger.fine("Opened TCP connection to leader");
				InputStream inFromServer = client2server.getInputStream();
			    OutputStream outToServer = client2server.getOutputStream();
			    outToServer.write(msg.getNetworkPayload());
			    logger.fine("Message sent to leader");
			    responseFromLeader = new Message(Util.readAllBytesFromNetwork(inFromServer));
			    logger.fine("Response from Leader received:\n"+responseFromLeader.toString());
			    client2server.close(); 
			} catch (Exception e) {
				logger.fine("Error! "+e.toString());
				if(this.retryCount < 5) {
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					logger.info("Gateway to Leader TCP socket could not be created. Is Leader dead? This is the "+ this.retryCount +" retry for request id "+requestID+". Try again with already read code");
					this.tpe.execute(new RunnableExchange(this.tpe, this.gateway, this.exchange, this.requestID, this.logger, this.retryCount + 1, data));
					return;
				} else {
					logger.severe("Gateway to Leader TCP socket could not be created. Is Leader dead? This is the "+ this.retryCount +" retry for request id "+requestID+". Presume this is a persistant error");
				}
			}
			try {
				if(responseFromLeader == null) {
					logger.fine("Looks like a server error occured! response from leader is still null");
					exchange.sendResponseHeaders(500, 0);
				} else if(responseFromLeader.getErrorOccurred()) {
			    	logger.fine("Response had an error, send 400 to client");
			    	exchange.sendResponseHeaders(400, responseFromLeader.getMessageContents().length);
			    	data.os.write(responseFromLeader.getMessageContents());
			    } else {
			    	logger.fine("Response came back clean, send back a 200");
			    	exchange.sendResponseHeaders(200, responseFromLeader.getMessageContents().length);
			    	data.os.write(responseFromLeader.getMessageContents());
			    }
			    data.os.flush();
			    data.os.close();
				exchange.close();
			} catch (IOException e) {//we assume because the gateway will always be up, communication between client and gateway is stable. Errors are non recoverable
				logger.severe("Error sending respnse to client. This is non recoverable.\n"+e.toString());
				return;
			}
		}

		private RequestData processRequest(HttpExchange exchange) {
			try {
				return new RequestData(exchange);
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}		
	}
	
	private class RequestData{
		public InputStream bodyIn;
		public OutputStream os;
		public String code;
		public byte[] bytes;
		public String sourceHost;
		public int sourcePort;
		public String receivingHost;
		public int receivingPort;
		
		public RequestData(HttpExchange exchange) throws IOException{
			bodyIn = exchange.getRequestBody();
			os = exchange.getResponseBody();
			this.bytes = bodyIn.readAllBytes();
			this.code = new String(this.bytes);
			sourceHost = exchange.getRemoteAddress().getHostName();
			sourcePort = exchange.getRemoteAddress().getPort();
			receivingHost = exchange.getLocalAddress().getHostName();
			receivingPort = exchange.getLocalAddress().getPort();
		}
	}
	

}
