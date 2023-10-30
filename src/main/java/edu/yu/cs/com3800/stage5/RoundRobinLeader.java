package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.Util;

public class RoundRobinLeader extends Thread implements LoggingServer{

	private final ZooKeeperPeerServerImpl server;
	private final Map<Long,InetSocketAddress> peerIDtoAddress;
	private final Queue<InetSocketAddress> roundRobinQueue;
	ConcurrentHashMap<Long, Message> completedWorkByRequestID = new ConcurrentHashMap<>();
	ServerSocket servSock;
	
    private final int myTcpPort;
    private final Logger logger;
    static private AtomicInteger socketCount = new AtomicInteger(0);
    private ThreadPoolExecutor tpe = new ThreadPoolExecutor(3, Runtime.getRuntime().availableProcessors() * 2, 100L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    
    public RoundRobinLeader(ZooKeeperPeerServerImpl server, Map<Long,InetSocketAddress> peerIDtoAddress, Set<InetSocketAddress> observerSet, JavaRunnerFollower oldFollower) {
    	this.server = server;
    	this.myTcpPort = this.server.getTcpPort();
    	this.logger = initializeLogging(this.getClass().getSimpleName() + "-on-tcp-port-" + this.myTcpPort);
    	if(oldFollower != null && oldFollower.getCompletedUnsentWork() != null && oldFollower.getCompletedUnsentWork().size() != 0) {
    		HashSet<Long> msgIds = new HashSet<>();
	    	for(Message m : oldFollower.getCompletedUnsentWork()) {
	    		completedWorkByRequestID.put(m.getRequestID(), m);
	    		msgIds.add(m.getRequestID());
	    		logger.fine("Old work added:\n"+m.toString());
	    	}
	    	logger.info("Leader initialized with old work!\nIds are: " + msgIds.toString());
    	}
    	if(oldFollower != null) {
    		this.servSock = oldFollower.getServerSocket();
    	}
    	this.peerIDtoAddress = peerIDtoAddress;//udp addresses
    	this.roundRobinQueue = new LinkedList<InetSocketAddress>();
    	for(InetSocketAddress addr : this.peerIDtoAddress.values()) {
    		
    		if(!observerSet.contains(addr) && !server.isPeerDead(addr)) {
    			InetSocketAddress tcpAddr = new InetSocketAddress(addr.getHostName(), addr.getPort() + 2);
				roundRobinQueue.add(tcpAddr);
    		}
    	}
    	logger.fine("Target list: "+Arrays.toString(this.roundRobinQueue.toArray(new InetSocketAddress[0])));
    }
    
    public void shutdown() {
    	this.interrupt();
    }
    
    @Override
    public void run() {
    	logger.fine("First execute requests for each worker thread to send completed work before starting server");
    	for(InetSocketAddress target : roundRobinQueue) {
    		logger.fine("Executing new leader request object to target with port "+ target.getPort());
			tpe.execute(new LeaderRequest(server.getAddress(), target, completedWorkByRequestID, tpe));
    	}
    	logger.fine("running server");
    	if(this.servSock == null) {
			try {
				servSock = new ServerSocket(this.server.getTcpPort());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	while(!this.isInterrupted()) {
    		Socket clntSock = null;
			try {
				clntSock = servSock.accept();
				logger.fine("new socket created");
			} catch (IOException e) {
				e.printStackTrace();
			}
			InetSocketAddress targetAddr = null;
			synchronized(roundRobinQueue) {
				
				targetAddr = this.roundRobinQueue.poll();
				while(this.server.isPeerDead(targetAddr)) {
					logger.fine("target "+targetAddr.toString() +" has been marked dead. Dequeue another server");
					targetAddr = this.roundRobinQueue.poll();
				}
				this.roundRobinQueue.add(targetAddr);
			}
			if(targetAddr == null) {
				logger.severe("No worker that isnt dead... Exiting");
				return;
			}
			logger.fine("Round robin leader on port " + this.myTcpPort + " sending socket count "+socketCount.get()+" of new work to: " + targetAddr.toString());
    		tpe.execute(new ClientRequest(server, clntSock, roundRobinQueue, targetAddr, socketCount.getAndIncrement(), completedWorkByRequestID));
    	}
    	//cleanup
    	logger.fine("Server exiting");
    	try {
			servSock.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	for(Handler h : this.logger.getHandlers()) {
        	this.logger.removeHandler(h);
        }
    }
    
    private class LeaderRequest implements Runnable, LoggingServer {
    	InetSocketAddress myAddress;
    	InetSocketAddress workerTarget;
    	Logger logger;
    	ConcurrentHashMap<Long, Message> completedWorkByRequestID;
    	ThreadPoolExecutor tpe;
    	
    	public LeaderRequest(InetSocketAddress myAddress, InetSocketAddress target, ConcurrentHashMap<Long, Message> completedWorkByRequestID, ThreadPoolExecutor tpe) {
    		this.myAddress = myAddress;
    		this.completedWorkByRequestID = completedWorkByRequestID;
    		this.workerTarget = target;
    		this.tpe = tpe;
    		this.logger = initializeLogging(this.getClass().getSimpleName() + "-from-"+server.getTcpPort()+"-to-Host["+workerTarget.getHostName()+"]-Port-["+workerTarget.getPort()+"]");
    	}
    	
    	public LeaderRequest(InetSocketAddress myAddress, InetSocketAddress target, ConcurrentHashMap<Long, Message> completedWorkByRequestID, ThreadPoolExecutor tpe, Logger logger) {
    		this.myAddress = myAddress;
    		this.completedWorkByRequestID = completedWorkByRequestID;
    		this.workerTarget = target;
    		this.tpe = tpe;
    		this.logger = logger;
    	}
    	
    	@Override
    	public void run() {
    		logger.fine("target for completed work: Host[ " + workerTarget.getHostName() +"] Port ["+(workerTarget.getPort())+"]");
    		Socket leader2Worker;
			try {
				leader2Worker = new Socket(InetAddress.getByName(workerTarget.getHostName()), workerTarget.getPort());
			} catch (IOException e) {
				logger.fine("Unable to open connection to worker. Is worker dead? This does not seem recoverable: "+e.toString());
				return;
			}
    		logger.fine("Connection opened");
    		Message msg = new Message(MessageType.NEW_LEADER_GETTING_LAST_WORK, new byte[0], myAddress.getHostName(), myAddress.getPort(), workerTarget.getHostName(), workerTarget.getPort());
    		OutputStream os = null;
    		InputStream is = null;
    		try {
    			os = leader2Worker.getOutputStream();
    			is = leader2Worker.getInputStream();
    		} catch (IOException e) {
    			logger.fine("Unable to get input and output streams from Socket. This does not seem recoverable: " + e.toString());
    			try {
					leader2Worker.close();
				} catch (IOException e1) {
					logger.fine("Unable to close socket. returning");
				}
    			return;
    		}
    		logger.fine("Output stream aquired");
    		try {
				os.write(msg.getNetworkPayload());
			} catch (IOException e) {
				logger.fine("Failed to write message payload into the output stream. This does not seem recoverable: " + e.toString());
				try {
					leader2Worker.close();
				} catch (IOException e1) {
					logger.fine("Unable to close socket. returning");
				}
    			return;
			}
    		logger.fine("Message out written");
    		try {
				os.flush();
			} catch (IOException e) {
				logger.fine("Failed to flush output stream. This does not seem recoverable: " + e.toString());
				try {
					leader2Worker.close();
				} catch (IOException e1) {
					logger.fine("Unable to close socket. returning");
				}
    			return;
			}
    		logger.fine("stream flushed");
    		logger.fine("Input stream aquired");
    		byte[] response = Util.readAllBytesFromNetwork(is);
    		Message responseFromWorker = null;
    		try {
    			responseFromWorker = new Message(response);
    		} catch (Exception e) {
    			logger.fine("Exception interpreting bytes to Message object. Did the worker die? " + e.toString());
    		}
    		//If we got a completed work message back, ask again if there is any more completed work
    		if(responseFromWorker != null && !responseFromWorker.getErrorOccurred()) {
    			logger.fine("Message is a real response and the error flag is not set. Assume real work has been passed back. Message is: " + responseFromWorker.toString());
    			completedWorkByRequestID.put(responseFromWorker.getRequestID(), responseFromWorker);
    			logger.fine("Send another request to worker asking for more work if it exists");
    			tpe.execute(new LeaderRequest(this.myAddress, this.workerTarget, this.completedWorkByRequestID, this.tpe, this.logger));
    		} else if (responseFromWorker == null){
    			logger.fine("Response from worker is null. Do nothing");
    		} else {
    			logger.fine("responseFromWorker error bit is set to true, meaning there is no work completed");
    		}
    		try {
				leader2Worker.close();
			} catch (IOException e1) {
				logger.fine("Unable to close finished socket. returning");
			}
			return;
    	}
    }
    
    private class ClientRequest implements Runnable, LoggingServer{
    	private final ZooKeeperPeerServerImpl server;
    	final Socket clientSocket;
    	InetSocketAddress workerTarget;
    	Logger logger;
    	ConcurrentHashMap<Long, Message> completedWorkByRequestID;
    	Queue<InetSocketAddress> targetQueue;
    	
    	public ClientRequest(ZooKeeperPeerServerImpl server, Socket clientSocket, Queue<InetSocketAddress> targetQueue, InetSocketAddress target, int socketCount, ConcurrentHashMap<Long, Message> completedWorkByRequestID) {
    		this.server = server;
    		this.targetQueue = targetQueue;
    		this.completedWorkByRequestID = completedWorkByRequestID;
    		this.clientSocket = clientSocket;
    		this.workerTarget = target;
    		this.logger = initializeLogging(this.getClass().getSimpleName() + "-socket-count-"+socketCount); 
    	}
		@Override
		public void run() {
			logger.fine("dequeued target for work: Host[ " + workerTarget.getHostName() +"] Port ["+(workerTarget.getPort())+"]");
			InputStream inFromClient = null;
			OutputStream outToClient = null;
			try {
				inFromClient = clientSocket.getInputStream();
				outToClient = clientSocket.getOutputStream();
			} catch (IOException e) {
				e.printStackTrace();
			}
			Message msgFromClient = null;
			try {
				msgFromClient = new Message(Util.readAllBytesFromNetwork(inFromClient));
			} catch (Exception e) {//communication from the gateway should be stable. Assumtpion is the gateway doesnt go down
				logger.fine("Error reading in data from gateway!\n"+e.toString());
				try {
					clientSocket.close();
				} catch (Exception e1) {
					logger.fine("Error closing client socket!\n"+e1.toString());
					return;
				}
			}
		    logger.fine("Received Message from client: \n"+msgFromClient.toString());
		    switch(msgFromClient.getMessageType()) {
			case COMPLETED_WORK:
				logger.fine("Receiving completed work from worker. This shouldnt happen unprompted!");
				this.completedWorkByRequestID.put(msgFromClient.getRequestID(), msgFromClient);
				break;
			case ELECTION:
				break;
			case GOSSIP:
				break;
			case NEW_LEADER_GETTING_LAST_WORK:
				break;
			case WORK:
					if(this.completedWorkByRequestID.containsKey(msgFromClient.getRequestID())) {
						logger.fine("Incoming request has previously been processed!");
						logger.info("RoundRobinLeader has found a cache hit on requestID: "+msgFromClient.getRequestID()+"!");
						try {
						outToClient.write(this.completedWorkByRequestID.get(msgFromClient.getRequestID()).getNetworkPayload());
						outToClient.flush();
						outToClient.close();
						this.completedWorkByRequestID.remove(msgFromClient.getRequestID());
						} catch (Exception e){
							logger.fine("error writing to client... \n"+e.toString());
							//communication to gateway should be stable. No recovery
							//break to exit while loop where client socket will be closed
							break;
						}
					} else {
						logger.fine("setting up to pass client data to worker");
						boolean complete = false;
						byte[] responseFromWorker = null;
						while(!complete) {
							if(responseFromWorker == null) {
								try {
									Socket leaderToWorker = new Socket(InetAddress.getByName(workerTarget.getHostName()), workerTarget.getPort());
									InputStream inFromWorker = leaderToWorker.getInputStream();
								    OutputStream outToWorker = leaderToWorker.getOutputStream();
								    outToWorker.write(msgFromClient.getNetworkPayload());
								    outToWorker.flush();
								    logger.fine("flushed data to worker");
								    responseFromWorker = Util.readAllBytesFromNetwork(inFromWorker);
								    leaderToWorker.close();
								    logger.fine("Work complete received from worker");
								} catch (IOException e) {
									logger.fine("there seems to have been an error sending/receiving data from leader to worker. Retry\n"+e.toString());
									synchronized(targetQueue) {
										workerTarget = this.targetQueue.poll();
										while(this.server.isPeerDead(workerTarget)) {
											workerTarget = this.targetQueue.poll();
										}
										this.targetQueue.add(workerTarget);
									}
								}
							}
							try {
							    outToClient.write(responseFromWorker);
							    outToClient.flush();
							    logger.fine("flush data back to client");
							    complete = true;
							} catch (Exception e) {
								logger.fine("there seems to have been an error sending data from leader to gateway. Store results and exit");
								try {
									Message resp = new Message(responseFromWorker);
									this.completedWorkByRequestID.put(resp.getRequestID(), resp);
								} catch (Exception e1) {
									logger.fine("Error parsing byte array of response into Message. Work discarded\n"+e1.toString());
								}
								complete = true;
							}
						}	
					}
				break;
		    }
		    try {
				clientSocket.close();
			} catch (Exception e) {
				logger.fine("Error closing socket from client before exiting!\n"+e.toString());
			}
		    logger.fine("ClientRequest finished processing");
		    for(Handler h : this.logger.getHandlers()) {
	        	this.logger.removeHandler(h);
	        }
		}
    }
}

