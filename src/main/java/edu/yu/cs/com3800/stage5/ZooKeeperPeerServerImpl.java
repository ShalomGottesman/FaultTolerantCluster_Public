package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.HttpHandlers.GossipTextHandler;
import edu.yu.cs.com3800.HttpHandlers.GossiplogLocationHandler;
import edu.yu.cs.com3800.HttpHandlers.HasLeaderHandler;
import edu.yu.cs.com3800.HttpHandlers.StateHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.net.httpserver.HttpServer;

public class ZooKeeperPeerServerImpl extends Thread implements ZooKeeperPeerServer{
    private final InetSocketAddress myAddress;
    private final int myUdpPort;
    private final int myTcpPort;
    private final int myHttpPort;
    private volatile boolean shutdown;
    private final Long id;
    protected long peerEpoch;
    protected final HashMap<Long,InetSocketAddress> peerIDtoAddress;
    private final Set<InetSocketAddress> observerSet;
    private final Logger logger;
    private final int observerCount;
    
    private LinkedBlockingQueue<Message> outgoingMessages = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<Message> incomingElectionMessages = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<Message> incomingGossipMessages = new LinkedBlockingQueue<>();
    
    protected final Gossip gossip;
    private final UDPMessageSender senderWorker;
    private final UDPMessageReceiver receiverWorker;
    
    private JavaRunnerFollower follower;
    private RoundRobinLeader leader;
    
    volatile HashSet<InetSocketAddress> deadSockets = new HashSet<>();
    volatile HashSet<Long> deadIDs = new HashSet<>();
    
    protected final Object stateChangeObject = new Object();
    
    //The following fields should all be changed together atomically
    protected volatile Vote currentLeader;
    private volatile ServerState state = ServerState.LOOKING;
    CountDownLatch stateChange = new CountDownLatch(1);
	HashMap<ServerState, CountDownLatch> fromState2Latch = new HashMap<>();
	HashMap<ServerState, CountDownLatch> toState2Latch = new HashMap<>();
	
	//The following fields are used together for failed servers
	protected HashMap<Long, CountDownLatch> failByID = new HashMap<>();
	protected HashSet<CountDownLatch> failByCount = new HashSet<>();
	protected final Object failLock = new Object();
	
	HttpServer httpServer;
    
	public static void main(String[] args) throws IOException {
		if(args.length != 4) {
			throw new IllegalArgumentException("ZookeeperPeerServer requires 4 arguments to run!");
		}
		int epoch = Integer.parseInt(args[0]);
		long id = Long.parseLong(args[1]);
		String[] mapEntires = args[2].replace("[", "").replace("]", "").split(",");
		HashMap<Long, InetSocketAddress> map = new HashMap<>();
		for(String str : mapEntires) {
			String[] data = str.split(":");
			map.put(Long.parseLong(data[0]), new InetSocketAddress(data[1], Integer.parseInt(data[2])));
		}
		String[] observerSetEntires = args[3].replace("[", "").replace("]", "").split(",");
		HashSet<InetSocketAddress> set = new HashSet<>();
		for(String str : observerSetEntires) {
			String[] data = str.split(":");
			set.add(new InetSocketAddress(data[0], Integer.parseInt(data[1])));
		}
		ZooKeeperPeerServerImpl zkpsi = new ZooKeeperPeerServerImpl(epoch, id, map, set);
		zkpsi.start();
		try {
			Thread.sleep(Long.MAX_VALUE);
		} catch (InterruptedException e) {
			zkpsi.shutdown();
		}
	}
	
	/**
	 * Constructor assumes the port is not included in the map. Adds the port to the map and calls alternative constructor
	 * @param port
	 * @param peerEpoch
	 * @param id
	 * @param peerIDtoAddress
	 * @param observerSet
	 * @throws IOException
	 */
	public ZooKeeperPeerServerImpl(int port, long peerEpoch, Long id, HashMap<Long,InetSocketAddress> peerIDtoAddress, Set<InetSocketAddress> observerSet) throws IOException{
		this(peerEpoch, id, addToMap(port, id, peerIDtoAddress), observerSet);
	}
	
	/**
	 * Bit hacky, but whatever
	 * @param port
	 * @param id
	 * @param map
	 * @return
	 */
	private static HashMap<Long,InetSocketAddress> addToMap(int port, Long id, HashMap<Long,InetSocketAddress> map){
		map.put(id, new InetSocketAddress("localhost", port));
		return map;
	}

	/**
	 * Constructor that assumes this server's address data is included in the peerIDtoAddress map and uses the provided server ID to derive its own address from said map
	 * @param peerEpoch
	 * @param id
	 * @param peerIDtoAddress
	 * @param observerSet
	 * @throws IOException
	 */
    public ZooKeeperPeerServerImpl(long peerEpoch, Long id, HashMap<Long,InetSocketAddress> peerIDtoAddress, Set<InetSocketAddress> observerSet) throws IOException{
    	this(peerEpoch, id, peerIDtoAddress, observerSet, ZooKeeperPeerServerImpl.class.getSimpleName() + "-on-udp-port-" + peerIDtoAddress.get(id).getPort());
    }
    
    protected ZooKeeperPeerServerImpl(long peerEpoch, Long id, HashMap<Long,InetSocketAddress> peerIDtoAddress, Set<InetSocketAddress> observerSet, String loggerName) throws IOException{
    	for(InetSocketAddress addr : observerSet) {
			if(!peerIDtoAddress.values().contains(addr)) {
				throw new IllegalArgumentException("All observer addresses must be in the peerIdtoAddress Map! " + addr.toString() + " was not in the map");
			}
		}	
    	InetSocketAddress myAddress = peerIDtoAddress.remove(id);
    	this.myUdpPort = myAddress.getPort();
    	this.myHttpPort = this.myUdpPort + 1;
    	this.myTcpPort = this.myUdpPort + 2;
    	this.myAddress = new InetSocketAddress("localhost", this.myUdpPort);
		this.id = id;
		this.peerIDtoAddress = peerIDtoAddress;
		this.observerSet = observerSet;	
		this.peerEpoch = peerEpoch;
		this.logger = initializeLogging(loggerName);
		this.senderWorker = new UDPMessageSender(this.outgoingMessages, this.myUdpPort);
    	this.senderWorker.setDaemon(true);
		this.receiverWorker = new UDPMessageReceiver(this.incomingElectionMessages, this.incomingGossipMessages, this.myAddress, this.myUdpPort, this);
		this.receiverWorker.setDaemon(true);
    	this.gossip = new Gossip(this, incomingGossipMessages, peerIDtoAddress);
    	this.observerCount = observerSet.size();
    	for(ServerState s : ServerState.values()) {
    		fromState2Latch.put(s, new CountDownLatch(1));
    		toState2Latch.put(s, new CountDownLatch(1));
    	}
    	startHttpServer();
    }
    
    @Override
	public void startHttpServer() throws IOException {
    	httpServer = HttpServer.create(new InetSocketAddress("localhost", this.myHttpPort), 0);
    	httpServer.createContext(ZooKeeperPeerServer.GOSSIP_LOG_PATH, new GossiplogLocationHandler(this.gossip));
    	httpServer.createContext(ZooKeeperPeerServer.GOSSIP_PATH, new GossipTextHandler(this.gossip));
    	httpServer.createContext(ZooKeeperPeerServer.SERVER_ROLE_PATH, new StateHandler(this));
    	httpServer.createContext(ZooKeeperPeerServer.LEADER_PATH, new HasLeaderHandler(this));
    	httpServer.setExecutor(null);		
	}
    
    

    @Override
    public void shutdown(){
    	logger.fine("Server shutting down");
        this.shutdown = true;
        this.senderWorker.shutdown();
        this.receiverWorker.shutdown();
        this.gossip.shutdown();
        this.httpServer.stop(1);
        if(this.leader != null) this.leader.shutdown();
        if(this.follower != null) this.follower.shutdown();
        for(Handler h : this.logger.getHandlers()) {
        	this.logger.removeHandler(h);
        }
        interrupt();
    }
    
    @Override
    public boolean isShutdown() {
    	return this.shutdown;
    }
    
    protected void startWorkerThreads() {
    	this.senderWorker.start();
    	this.receiverWorker.start();
    	this.gossip.start();
    	this.httpServer.start();
    }
    
    protected ZooKeeperLeaderElection getZooKeeperLeaderElection() {
    	return new ZooKeeperLeaderElection(this, this.incomingElectionMessages, this.getPeerState() == ServerState.OBSERVER);
    }
    
    @Override
    public void run(){
    	logger.fine("Server starting");
    	startWorkerThreads();
    	logger.finest("Message receiver and sender threads started");
        //step 3: main server loop
        while (!this.shutdown){
            switch (getPeerState()){
                case LOOKING:
                    //start leader election, set leader to the election winner
                	logger.fine("Starting new Election!");
                	this.setCurrentLeader(getZooKeeperLeaderElection().lookForLeader());
                	logger.fine("Election finished");
                	continue;//after new leader is found, we want to switch over the state again immediately
                case LEADING:
                	logger.fine("Leading. Start leader thread");
                	this.leader = new RoundRobinLeader(this, (HashMap<Long, InetSocketAddress>)this.peerIDtoAddress.clone(), this.observerSet, this.follower);
                	this.leader.start();
                	this.follower = null;
                	break;
                case OBSERVER:
                	break;
                case FOLLOWING:
                	logger.fine("Following. Start following thread");
				try {
					this.follower = new JavaRunnerFollower(this, this.follower);
				} catch (IOException e1) {
					logger.warning("Server peer failed to start a follower instance!\n"+e1.toString());
					e1.printStackTrace();
				}
                	this.follower.start();
                	break;
            }
            if(!this.shutdown) {
            	logger.fine("shutdown flag is false, entering wait on lock object.");
            	synchronized(stateChangeObject) {
            		try {
            			stateChangeObject.wait();
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
	public void sendMessage(MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {
    	logger.finest("Sending Message Type ["+type+"] to Target ["+target.toString()+"]");
    	Message msg = new Message(type, messageContents, this.myAddress.getHostString(), this.myUdpPort, target.getHostString(), target.getPort());
    	try {
			this.outgoingMessages.put(msg);
		} catch (InterruptedException e) {
			//thrread likely interrupted because of shutdown. re-interrupt
			interrupt();
		}
    }

	@Override
	public void sendBroadcast(MessageType type, byte[] messageContents) {
		logger.finest("Sending broadcast message of type: " + type);
		for(InetSocketAddress target : this.peerIDtoAddress.values()) {
			this.sendMessage(type, messageContents, target);
		}
	}
	@Override
	public void setCurrentLeader(Vote v) {
		if(v == null) {
			//set leader to null, implies FOLLOWING server state
			logger.fine("Setting current leader to null");
			synchronized(this) {
				this.currentLeader = v;
				this.setPeerState(ServerState.LOOKING);
			}
		} else {
			logger.fine("Setting current leader \n"+v.myToString());
			logger.log(Level.INFO, "Server with ID " + this.getServerId() + " is setting current Leader to " + v.myToString());
			synchronized(this) {
				this.currentLeader = v;
				this.peerEpoch = v.getPeerEpoch();
				if(this.getPeerState() != ServerState.OBSERVER) {
			        //set my state to either LEADING or FOLLOWING
			    	if(v.getProposedLeaderID() == this.getServerId()) {
			    		logger.fine("proposed ID of winner in notification matches my server. set to LEADING");
			    		this.setPeerState(ServerState.LEADING);
			    	} else {
			    		logger.fine("proposed ID of winner in notification does not match my server. set to FOLLOWING");
			    		this.setPeerState(ServerState.FOLLOWING);
			    	}
		    	}
			}
		}
	}
	
	@Override
	public void setPeerState(ServerState newState) {
		logger.fine("Setting peer state to: " + newState +" from "+this.state);
		System.out.println("Setting peer state of server id "+getServerId() +" to: " + newState +" from "+this.state);
		ServerState oldState = this.state;
		this.state = newState;
		if(this.state == ServerState.LOOKING) {
			logger.fine("Shut down leader and follower threads");
			if(this.leader != null && this.leader.isAlive()) {
        		this.leader.shutdown();
        	}
			if(this.follower != null && this.follower.isAlive()) {
        		this.follower.shutdown();
        	}
		}
		if(oldState != ServerState.LOOKING) {
			logger.fine("notifying on lock object");
			synchronized(stateChangeObject) {
				stateChangeObject.notify();
			}
			logger.fine("lock notified on");
		} else {
			logger.fine("Server was previously in LOOKING state, no need to notify");
		}
		if(oldState == newState) {
			decrement(this.stateChange);
			this.stateChange = new CountDownLatch(1);
		}
		decrement(this.fromState2Latch.get(oldState));
		this.fromState2Latch.put(oldState, new CountDownLatch(1));
		decrement(this.toState2Latch.get(newState));
		this.toState2Latch.put(newState, new CountDownLatch(1));
		logger.fine("state changed to "+this.state);
	}

	public synchronized CountDownLatch registerForServerStateChange() {
		logger.finest("requested server state change latch. Returning: " + this.stateChange.toString());
		return this.stateChange;
	}

	public synchronized CountDownLatch registerForServerStateChangeFrom(ServerState state) {
		logger.finest("requested server state change latch from "+state+". Returning: " + this.fromState2Latch.get(state).toString());
		return this.fromState2Latch.get(state);
	}

	public synchronized CountDownLatch registerForServerStateChangeTo(ServerState state) {
		logger.finest("requested server state change latch to "+state+". Returning: " + this.fromState2Latch.get(state).toString());
		return this.toState2Latch.get(state);
	}

	@Override
	public Vote getCurrentLeader() {
		if(this.currentLeader!= null) {
			logger.fine("requested current leader, returning\n"+this.currentLeader.myToString());
		} else {
			logger.fine("requested current leader, returning\nnull");
		}
		return this.currentLeader;
	}

	@Override
	public ServerState getPeerState() {
		return this.state;
	}	

	private void decrement(CountDownLatch cdl) {
		if(cdl != null) {
			cdl.countDown();
			logger.finest("decremented latch: " + cdl.toString());
		}
	}

	@Override
	public Long getServerId() {
		return this.id;
	}

	@Override
	public long getPeerEpoch() {
		return this.peerEpoch;
	}

	@Override
	public InetSocketAddress getAddress() {
		return this.myAddress;
	}

	@Override
	public int getUdpPort() {
		return this.myUdpPort;
	}
	
	public int getTcpPort() {
		return this.myTcpPort;
	}

	@Override
	public InetSocketAddress getPeerByID(long peerId) {
		return peerIDtoAddress.get(peerId);
	}

	@Override
	public int getQuorumSize() {
		return ((this.peerIDtoAddress.size() - this.deadIDs.size() + 1 - this.observerCount) / 2) + 1;
	}
	
	public Logger getLogger() {
    	return this.logger;
    }
	
	public CountDownLatch registerGeneralFailCount(int count) {
		CountDownLatch cdl = new CountDownLatch(count);
		synchronized(this.failLock) {
			this.failByCount.add(cdl);
		}
		return cdl;
	}

	public CountDownLatch registerForServerFail(long id) {
		CountDownLatch cdl = new CountDownLatch(1);
		synchronized(this.failLock) {
			if(this.failByID.containsKey(id)) {
				return this.failByID.get(id);
			} else {
				this.failByID.put(id,  cdl);
				return cdl;
			}
		}
	}

	@Override
	public void reportFailedPeer(long peerID) {
		logger.fine("Peer with id "+peerID+" marked dead!");
		deadIDs.add(peerID);
		deadSockets.add(this.getPeerByID(peerID));
		if(this.state == ServerState.FOLLOWING && peerID == this.currentLeader.getProposedLeaderID()) {
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

	@Override
	public boolean isPeerDead(long peerID) {
		return this.deadIDs.contains(peerID);
	}

	@Override
	public boolean isPeerDead(InetSocketAddress address) {
		return this.deadSockets.contains(address);
	}

	@Override
	public int deadPeerCount() {
		return this.deadIDs.size();
	}

	public int getHttpPort() {
		return this.myHttpPort;
	}
}
