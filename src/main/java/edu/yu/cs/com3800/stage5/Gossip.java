package edu.yu.cs.com3800.stage5;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Message.MessageType;

public class Gossip extends Thread implements LoggingServer{
	ZooKeeperPeerServerImpl server;
	Logger logger;
	Logger gossipLogger;
	LinkedBlockingQueue<Message> gossipIncoming;
	Map<Long,InetSocketAddress> peerIDtoAddress;
	Queue<InetSocketAddress> targetQueue = new LinkedList<>();
	final Long serverID;
	StringBuilder gossipReadable = new StringBuilder();
	
	//Default values. Constructor sets them based on System properties
	static final int GOSSIP = 3000;
	static final int FAIL = GOSSIP * 10;
	static final int CLEANUP = FAIL * 2;
	
	private long heatbeatCounter = 0;
	private HashMap<Long, Long> foreignID2Heatbeat = new HashMap<>();
	private HashMap<Long, Long> foreignID2Time = new HashMap<>();;
	private HashSet<Long> markedSet = new HashSet<>();
	private final long localStartTime;
	
	private boolean running = true;
	
	public Gossip(ZooKeeperPeerServerImpl server, LinkedBlockingQueue<Message> gossipIncoming, Map<Long,InetSocketAddress> peerIDtoAddress) {
		localStartTime = System.currentTimeMillis();
		this.server = server;
		this.gossipIncoming = gossipIncoming;
		this.logger = initializeLogging(this.getClass().getSimpleName() + "-on-udp-port-"+server.getUdpPort());
		this.gossipLogger = initializeLogging("summery-"+this.getClass().getSimpleName() + "-on-udp-port-"+server.getUdpPort());
		this.peerIDtoAddress = peerIDtoAddress;
		this.serverID = this.server.getServerId();
		HashSet<InetSocketAddress> targetSet = new HashSet<>();
		for(InetSocketAddress t : this.peerIDtoAddress.values()) {
			targetSet.add(t);
		}
		InetSocketAddress[] targets = targetSet.toArray(new InetSocketAddress[0]);
		shuffleArray(targets);
		logger.fine("Randomized target order:\n"+Arrays.toString(targets));
		for(InetSocketAddress target : targets) {
			targetQueue.add(target);
		}
	}
	
	public void shutdown() {
		running = false;
		for(Handler h : this.logger.getHandlers()) {
        	this.logger.removeHandler(h);
        }
		interrupt();
	}
	
	private long getLocalTime() {
		return System.currentTimeMillis() - this.localStartTime;
	}
	
	public String getGossipContents() {
		return this.gossipReadable.toString();
	}
	
	public String getGossipReadLogLocation() {
		return LoggingServer.log2fileLocation.get(this.gossipLogger);
	}
	
	@Override
	public void run() {
		long nextGossipTime = getLocalTime();
		long nextTableProcessTime = getLocalTime();
		long now = getLocalTime();
		logger.fine("Local start time is: "+localStartTime+". Next gossip sent set to: " + nextGossipTime + ". next table analysis time: " + nextTableProcessTime);
		while(running) {
			//start with sending out gossip message
			InetSocketAddress target = targetQueue.poll();
			if(target != null) {
				logger.fine("dequed target: "+target.toString());
			} else {
				logger.fine("dequed target is null!");
			}
			while((target == null || server.isPeerDead(target)) && targetQueue.size() > 0) {
				logger.fine("target failed requirments, retrying");
				target = targetQueue.poll();
			}
			if(target == null) {
				logger.fine("Queue ran out of target... return?");
				return;
			}
			//re-add target for later use
			targetQueue.add(target);
			byte[] contents = getGossipBuffer();
			try {
				long sleepTime = nextGossipTime - getLocalTime();
				logger.fine("Sleeping in prep for next gossip out. Sleeping for: "+sleepTime+". If negetive, skip");
				if(sleepTime > 0) {
					Thread.sleep(sleepTime);
				}
			} catch (InterruptedException e1) {
				logger.fine("Thread interrupted from sleep before sending Gossip. Likely a shutdown is occuring");
				interrupt();
			}
			logger.fine("sending gossip message");
			server.sendMessage(MessageType.GOSSIP, contents, target);
			//Record next time a gossip message has to go out
			nextGossipTime = getLocalTime() + GOSSIP;
			logger.fine("setting next gossip to: " + nextGossipTime);
			//dequeue incoming gossip messages until I need to send another one myself
			//provide a 300 millisend buffer in prep for gossip out 
			while(nextGossipTime - getLocalTime()  - 300 > 0) {
				logger.fine("loop to process table and incoming messages");
				now = getLocalTime();
				//check if its time to process the table
				if(now >= nextTableProcessTime) {
					logger.fine("time passed to process table");
					//processTable() returns the next time, based on its currect info, that it has to be checked (Oldest entry at check time + FAIL time)
					nextTableProcessTime = processTable();
					logger.fine("setting next time to process table at: " + nextTableProcessTime);
				} else {
					logger.fine("now is not past time to check table! Now is: " + now+". Time to check the table is "+ nextTableProcessTime);
				}
				Message gossipIn = null;
				try {
					//Dont want to wait too long that I miss the next gossip out time.
					long waitForMsg = (nextGossipTime - getLocalTime() - 200) / 2;
					logger.fine("polling for gossip msg with max wait: " + waitForMsg+". If negetive dont try to wait");
					if(waitForMsg > 0) {
						gossipIn = this.gossipIncoming.poll(waitForMsg, TimeUnit.MILLISECONDS);
					} else {
						gossipIn = this.gossipIncoming.poll();
					}
				} catch (InterruptedException e) {
					logger.fine("Thread interrupted from waiting for a Gossip in. Likely a shutdown is occuring");
				}
				if(gossipIn == null) {
					logger.fine("Msg came back null, next");
					continue;
				} else {
					logger.fine("Message came in as: " + gossipIn.toString());
				}
				//process message
				processContents(gossipIn.getMessageContents());
			}
		}
		logger.log(Level.SEVERE, "Gossip thread on server with id " + this.serverID +" exiting");
	}
	/**
	 * Processes the entries of the table and returns timestamp of next check
	 * @return
	 */
	private long processTable() {
		logger.finer("Starting table processing");
		long oldestEntry = Long.MAX_VALUE;
		HashSet<Long> keysToRemove = new HashSet<>();
		for(Long id : foreignID2Time.keySet()) {
			long oldTime = foreignID2Time.get(id);
			long currentTime = getLocalTime();
			logger.finest("id["+id+"] old time["+oldTime+"] currentTime["+currentTime+"] relation["+(currentTime - oldTime)+"]");
			if(currentTime - oldTime < FAIL) {
				if(oldTime < oldestEntry) {
					oldestEntry = oldTime;
				}
				logger.finest("id["+id+"] time stamp is beneath fail time of: " + FAIL);
				markedSet.remove(id);
			} else if(currentTime - oldTime >= FAIL && currentTime - oldTime < CLEANUP) {
				if(!this.markedSet.contains(id)) {
					logger.fine("id["+id+"] time stamp is past fail time ["+FAIL+"] but before cleanup time ["+CLEANUP+"]");
					logger.log(Level.WARNING, "server with ID["+this.serverID+"] is marking server ["+id+"] as failed!");
					this.server.reportFailedPeer(id);
					markedSet.add(id);
				}
			} else {
				logger.fine("id["+id+"] time stamp is past cleanup time: " + CLEANUP);
				keysToRemove.add(id);
				markedSet.remove(id);
			}
		}
		for(Long id : keysToRemove) {
			foreignID2Heatbeat.remove(id);
			foreignID2Time.remove(id);
		}
		if(oldestEntry == Long.MAX_VALUE) {
			long nextCheck = getLocalTime() + FAIL;
			logger.fine("returning next table process time: "+ nextCheck);
			return nextCheck;
		} else {
			logger.fine("returning next table process time: "+ (oldestEntry + FAIL));
			return oldestEntry + FAIL;
		}
	}

	private void processContents(byte[] contents) {
		logger.fine("starting processing for gossip contents");
		ByteBuffer buffer = ByteBuffer.wrap(contents);
		StringBuilder builder = new StringBuilder();
		for(int x = 0; x < buffer.capacity() / 8; x+=2) {
			long serverID = buffer.getLong();
			long serverHeartBeat = buffer.getLong();
			if(x == 0) {//if first entry...
				builder.append("Gossip from ID ["+serverID+"] at time: " + System.currentTimeMillis()+"\n");
			}
			builder.append("\tEntry: ID["+serverID+"] Heartbeat ["+serverHeartBeat+"]\t");
			long time = getLocalTime();
			logger.fine("entry: ID["+serverID+"] HEARBEAT["+serverHeartBeat+"] current time["+time+"]");
			if(markedSet.contains(serverID)){ 
				logger.fine("server id already marked, continuing");
				builder.append("\n");
				continue;
			} else if(serverID == this.serverID) {
				logger.fine("server id matches this server. continuing");
				builder.append("\n");
				continue;
			} else if(foreignID2Heatbeat.containsKey(serverID)) {//update old entry
				long prevBeat = foreignID2Heatbeat.get(serverID);
				if(prevBeat >= serverHeartBeat) {
					logger.fine("instnace of heart beat is not greater than current entry. continue");
					builder.append("\n");
					continue;
				} else {
					logger.fine("heart beat for this server is greater than old entry. Update");
					builder.append("Update from heartbeat ["+foreignID2Heatbeat.get(serverID)+"] to ["+serverHeartBeat+"]!");
					foreignID2Heatbeat.put(serverID, serverHeartBeat);
					foreignID2Time.put(serverID, time);
				}
			} else {//new entry
				logger.fine("new heart beat entry!");
				builder.append("New entry!");
				foreignID2Heatbeat.put(serverID, serverHeartBeat);
				foreignID2Time.put(serverID, time);
			}
			builder.append("\n");
		}
		String gossip = builder.toString();
		this.gossipLogger.fine(gossip);
		this.gossipReadable.append(gossip);
	}
	
	private byte[] getGossipBuffer() {
		StringBuilder builder = new StringBuilder();
		/*
		 * Size of buffer = 
		 * 1 long (sender ID) = 8 bytes
		 * 1 long (sender heatbeat counter) = 8 bytes
		 * for each entry ID in table:
		 * 1 long (entry server ID) = 8 bytes
		 * 1 long (entry server heartbeat) = 8 bytes
		 */
		int bufferSize = 16 + ((foreignID2Heatbeat.keySet().size() - this.markedSet.size()) * 16);
		ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
		buffer.clear();
		buffer.putLong(this.serverID);
		buffer.putLong(this.heatbeatCounter);
		builder.append("ID["+this.serverID+"]:HeartBeat["+this.heatbeatCounter+"]\n");
		this.heatbeatCounter++;
		for(Long id : foreignID2Heatbeat.keySet()) {
			if(this.markedSet.contains(id)) {
				continue;
			}
			buffer.putLong(id);
			buffer.putLong(foreignID2Heatbeat.get(id));
			builder.append("ID["+id+"]:HeartBeat["+foreignID2Heatbeat.get(id)+"]\n");
		}
		logger.finest("returning Gossip buffer:\n"+builder.toString());
		buffer.flip();
		return buffer.array();
	}
	
	// Implementing Fisher–Yates shuffle
	private void shuffleArray(InetSocketAddress[] ar) {
		// If running on Java 6 or older, use `new Random()` on RHS here
		Random rnd = ThreadLocalRandom.current();
		for (int i = ar.length - 1; i > 0; i--){
			int index = rnd.nextInt(i + 1);
			// Simple swap
			InetSocketAddress a = ar[index];
			ar[index] = ar[i];
			ar[i] = a;
		}
	}
	
}
