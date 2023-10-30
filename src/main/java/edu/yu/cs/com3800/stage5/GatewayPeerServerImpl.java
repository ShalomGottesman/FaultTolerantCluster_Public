package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.net.httpserver.HttpServer;

import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.UDPMessageReceiver;
import edu.yu.cs.com3800.UDPMessageSender;
import edu.yu.cs.com3800.Vote;
import edu.yu.cs.com3800.ZooKeeperLeaderElection;
import edu.yu.cs.com3800.ZooKeeperPeerServer;
import edu.yu.cs.com3800.HttpHandlers.GatewayClusterStatusHandler;
import edu.yu.cs.com3800.HttpHandlers.GossipTextHandler;
import edu.yu.cs.com3800.HttpHandlers.GossiplogLocationHandler;
import edu.yu.cs.com3800.HttpHandlers.HasLeaderHandler;
import edu.yu.cs.com3800.HttpHandlers.StateHandler;
import edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState;

public class GatewayPeerServerImpl extends ZooKeeperPeerServerImpl{
	private final int gatewayHttpPort;

	Logger logger;
	private Object newleaderlock = new Object();
    public GatewayPeerServerImpl(int gatewayPort, long peerEpoch, Long id, HashMap<Long,InetSocketAddress> peerIDtoAddress, Set<InetSocketAddress> observerSet) throws IOException{
    	super(peerEpoch, id, peerIDtoAddress, observerSet, GatewayPeerServerImpl.class.getSimpleName() + "-on-udp-port-" + peerIDtoAddress.get(id).getPort());
    	this.gatewayHttpPort = gatewayPort;
    	this.logger = this.getLogger();
    }
    
	@Override
    public void run(){
    	logger.fine("Server starting");
    	startWorkerThreads();
    	logger.fine("Message receiver and sender threads started");
    	while(!this.isShutdown() && this.currentLeader == null) {
    		logger.fine("entering start if main while loop...");
    		//assume always observer...
			this.setCurrentLeader(getZooKeeperLeaderElection().lookForLeader());
    		if(!this.isShutdown() && this.currentLeader != null) {
    			logger.fine("shutdown flag is false, entering wait on lock object.");
            	synchronized(stateChangeObject) {
            		try {
            			stateChangeObject.wait();
            			logger.fine("Wait complete");
            		} catch(InterruptedException e) {
            			//do nothing. Likely means thread is exiting.
            		}
            	}
    		} else {
            	logger.fine("Shutdown flag is true, skipped wait!");
            }
    	}
    }
	
	@Override
	public void startHttpServer() throws IOException {
    	httpServer = HttpServer.create(new InetSocketAddress("localhost", this.getHttpPort()), 0);
    	httpServer.createContext(ZooKeeperPeerServer.GOSSIP_LOG_PATH, new GossiplogLocationHandler(this.gossip));
    	httpServer.createContext(ZooKeeperPeerServer.GOSSIP_PATH, new GossipTextHandler(this.gossip));
    	httpServer.createContext(ZooKeeperPeerServer.SERVER_ROLE_PATH, new StateHandler(this));
    	httpServer.createContext(ZooKeeperPeerServer.LEADER_PATH, new HasLeaderHandler(this));
    	httpServer.createContext(ZooKeeperPeerServer.GATEWAY_CLUSTER_STATUS, new GatewayClusterStatusHandler(this, peerIDtoAddress));
    	httpServer.setExecutor(null);		
	}
	

	@Override
	public ServerState getPeerState() {
		return ServerState.OBSERVER;
	}

	@Override
	public void setPeerState(ServerState newState) {
		decrement(this.stateChange);
		this.stateChange = new CountDownLatch(1);
		decrement(this.fromState2Latch.get(ServerState.OBSERVER));
		this.fromState2Latch.put(ServerState.OBSERVER, new CountDownLatch(1));
		decrement(this.toState2Latch.get(newState));
		this.toState2Latch.put(newState, new CountDownLatch(1));
	}
	
	private void decrement(CountDownLatch cdl) {
		if(cdl != null) {
			cdl.countDown();
		}
	}
	
	private CountDownLatch leaderChangeLatch = new CountDownLatch(1);
	
	@Override
	public void setCurrentLeader(Vote v) {
		if(v == null) {
			logger.fine("set current leader called with null");
			synchronized(this.stateChangeObject) {
				logger.fine("Set current leader to null");
				this.currentLeader = v;
				logger.fine("Notifying on state change lock");
				this.stateChangeObject.notify();
				logger.fine("state change lock notified on");
			}
		} else {
			logger.fine("Setting current leader \n"+v.myToString());
			logger.log(Level.INFO, "Server with ID " + this.getServerId() + " is setting current Leader to " + v.myToString());
			synchronized(this.stateChangeObject) {
				this.currentLeader = v;
				this.peerEpoch = v.getPeerEpoch();
				CountDownLatch old = this.leaderChangeLatch;
				old.countDown();
				leaderChangeLatch = new CountDownLatch(1);
				logger.fine("Counted down on latch "+old.toString()+". Reinitialized to latch: " + this.leaderChangeLatch.toString());
			}
			synchronized(this.newleaderlock) {
				this.newleaderlock.notifyAll();
			}
		}
	}
	
	public CountDownLatch getLeaderChangeLatch() {
		return this.leaderChangeLatch;
	}
	
	@Override
	public void reportFailedPeer(long peerID) {
		logger.fine("Peer with id "+peerID+" marked dead!");
		deadIDs.add(peerID);
		deadSockets.add(this.getPeerByID(peerID));
		if(this.currentLeader != null && peerID == this.currentLeader.getProposedLeaderID()) {
			logger.fine("trigger new election!");
			this.peerEpoch++;
			this.setCurrentLeader(null);
		}
		synchronized(this.failLock) {
			for(CountDownLatch cdl : this.failByCount) {
				cdl.countDown();
			}
			if(this.failByID.containsKey(peerID)) {
				this.failByID.get(peerID).countDown();
			}
		}
	}
	
	public InetSocketAddress getLeaderAddress() {
		if(this.getCurrentLeader() == null) {
			return null;
		}
		return this.getPeerByID(this.getCurrentLeader().getProposedLeaderID());
	}
	
	public Object getLeaderMonitor() {
		return this.newleaderlock;
	}

	public int getGatewayHttpPort() {
		return this.gatewayHttpPort;
	}
}
