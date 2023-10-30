package edu.yu.cs.com3800.stage5;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.logging.Handler;
import java.util.logging.Logger;

import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.Message.MessageType;

public class JavaRunnerFollower extends Thread implements LoggingServer{

	private final ZooKeeperPeerServerImpl server;
    private final Logger logger;
    HashSet<Message> completedWork = new HashSet<>();
    ServerSocket servSock;
    JavaRunner runner;
    
    public JavaRunnerFollower(ZooKeeperPeerServerImpl server, JavaRunnerFollower oldFollower) throws IOException {
    	this.server = server;
    	if(oldFollower != null) {
    		runner = oldFollower.runner;
    		logger = oldFollower.logger;
    		logger.fine("Taking over old follower on this server");
    	} else {
    		this.logger = initializeLogging(this.getClass().getSimpleName() + "-on-tcp-port-" + this.server.getTcpPort());
    		this.runner = new JavaRunner();
    	}
    	if(oldFollower != null && oldFollower.getCompletedUnsentWork() != null && oldFollower.getCompletedUnsentWork().size() != 0) {
    		logger.fine("moving over all work from old follower instance!");
    		HashSet<Long> msgIds = new HashSet<>();
    		for(Message msg : oldFollower.getCompletedUnsentWork()) {
    			msgIds.add(msg.getRequestID());
    			this.completedWork.add(msg);
    		}
    		logger.fine("New JavaRunnerFollower instance pulling in state from old follower!\nIds are: " + msgIds.toString());
    	} else if(oldFollower != null) {
    		logger.fine("started with old follower not null, but no old work being pulled");
    	}
    }
    
    public void shutdown() {
    	this.interrupt();
    	try {
			this.servSock.close();
			this.servSock = null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public HashSet<Message> getCompletedUnsentWork(){
    	return this.completedWork;
    }
    
    
    
    public ServerSocket getServerSocket() {
    	return this.servSock;
    }
    
    @Override
    public void run() {
    	if(this.servSock == null) {
			try {
				servSock = new ServerSocket(this.server.getTcpPort());
				logger.fine("Started up new server socket: " + servSock.toString());
			} catch (IOException e) {
				logger.fine("Error trying to initialize server socket!\n"+e.toString());
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	} else {
    		logger.fine("Server socket is not null, not trying to create a new one");
    	}
    	while(!this.isInterrupted()) {
    		Socket leaderSocket = null;
			try {
				leaderSocket = servSock.accept();
				logger.fine("new socket from leader");
			} catch (Exception e) {
				if(this.isInterrupted()) {//server shutdown started. No issues
					interrupt();
					break;
				} else {
					logger.fine("accept connection fail!\n"+e.toString());
					continue;
				}
			}
			InputStream inFromLeader = null;
			try {
				inFromLeader = leaderSocket.getInputStream();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			OutputStream outToLeader = null;
			try {
				outToLeader = leaderSocket.getOutputStream();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			Message msgFromLeader = null;
			try {
				msgFromLeader = new Message(Util.readAllBytesFromNetwork(inFromLeader));
			} catch (Exception e) {
				logger.fine("Error reading in Message from network\n"+e.toString());
				try {
					leaderSocket.close();
				} catch (Exception e1) {
					logger.fine("Error closing socket from leader\n"+e1.toString());
				}
				continue;
			}
			logger.fine("new Message is \n"+msgFromLeader.toString() + "\nMsg content:"+Arrays.toString(msgFromLeader.getMessageContents()));
    		switch(msgFromLeader.getMessageType()) {
			case COMPLETED_WORK:
				break;
			case ELECTION:
				break;
			case GOSSIP:
				break;
			case NEW_LEADER_GETTING_LAST_WORK:
				Message resp = null;
				if(this.completedWork.size() == 0) {
					//There is no work to be passed back, error bit is set to true, id to 0 and content to 0
					resp = new Message(MessageType.COMPLETED_WORK, new byte[0], server.getAddress().getHostName(), server.getTcpPort(), leaderSocket.getInetAddress().getHostAddress(), leaderSocket.getPort(), 0, true);
				} else {
					//there is work, but there may be more than one, so get a random one from the set and return it
					resp = getRandomFromSet();
				}
				logger.fine("Responding: "+resp.toString());
				try {
					outToLeader.write(resp.getNetworkPayload());
					logger.fine("response written back to leader");
				} catch (IOException e) {
					logger.fine("Error writign response back to leader!\n"+e.toString());
				}
				try {
					leaderSocket.close();
				} catch (IOException e) {
					logger.fine("Error closing socket back to leader!\n"+e.toString());
				}
				break;
			case WORK:
				String returnVal = "";
				boolean error = false;
				ByteArrayInputStream codeIn = new ByteArrayInputStream(msgFromLeader.getMessageContents());
				try {
					returnVal = runner.compileAndRun(codeIn);
				} catch(Exception e) {
					logger.warning("request with id: "+ msgFromLeader.getRequestID() +" on server with id "+this.server.getServerId()+" compiled and ran with an error!");
					error = true;
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					PrintStream ps = new PrintStream(baos);
					e.printStackTrace(ps);
					returnVal = e.getMessage() + '\n' + baos.toString(StandardCharsets.UTF_8);
				}
				Message response = new Message(MessageType.COMPLETED_WORK, returnVal.getBytes(), msgFromLeader.getReceiverHost(), msgFromLeader.getReceiverPort(), msgFromLeader.getSenderHost(), msgFromLeader.getSenderPort(), msgFromLeader.getRequestID(), error);
				logger.fine("completed work is:\n"+response.toString());
				completedWork.add(response);
				try {
					outToLeader.write(response.getNetworkPayload());
					completedWork.remove(response);
					logger.fine("Work complete written back to leader");
				} catch (IOException e) {
					logger.fine("Error writing response to Leader!\n"+e.toString());
				}
				try {
					leaderSocket.close();
				} catch (IOException e) {
					logger.fine("Error closing socket to Leader!\n"+e.toString());
				}
				break;    		
    		}
    	}
    	//cleanup
    	logger.fine("Server exiting");
    	for(Handler h : this.logger.getHandlers()) {
        	this.logger.removeHandler(h);
        }
    }
    private Message getRandomFromSet() {
		int size = this.completedWork.size();
		int item = new Random().nextInt(size);
		int i = 0;
		Message resp = null;
		for(Message obj : this.completedWork) {
		    if (i == item) {
		    	resp = obj;
		    	break;
		    }
		    i++;
		}
		this.completedWork.remove(resp);
		return resp;
	}
}
