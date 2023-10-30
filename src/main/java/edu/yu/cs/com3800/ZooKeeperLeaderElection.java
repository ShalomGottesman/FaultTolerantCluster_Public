package edu.yu.cs.com3800;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState;

public class ZooKeeperLeaderElection implements LoggingServer{
    /**
     * time to wait once we believe we've reached the end of leader election.
     */
    private final static int finalizeWait = 5000;

    /**
     * Upper bound on the amount of time between two consecutive notification checks.
     * This impacts the amount of time to get the system up again after long partitions. Currently 60 seconds.
     */
    private final static int maxNotificationInterval = 60000;

	private final LinkedBlockingQueue<Message> incomingMessages;

	private final ZooKeeperPeerServer myPeerServer;

	private long proposedLeader;

	private long proposedEpoch;
	
	private Map<Long, ElectionNotification> senderToNotification = new HashMap<>();
	
	private Logger logger;
	
	private boolean isObserver;

    public ZooKeeperLeaderElection(ZooKeeperPeerServer server, LinkedBlockingQueue<Message> incomingMessages, boolean observer)
    {
    	this.isObserver = observer;
        this.incomingMessages = incomingMessages;
        this.myPeerServer = server;
        this.proposedEpoch = server.getPeerEpoch();
        this.proposedLeader = server.getServerId();
        if(this.isObserver) {
        	this.proposedLeader = Long.MIN_VALUE;
        }
        this.logger = initializeLogging(this.getClass().getSimpleName()+"-for-(epoch,serverID)-("+server.getPeerEpoch()+","+this.myPeerServer.getServerId()+")");
        logger.fine("Quorum size is: "+ this.myPeerServer.getQuorumSize());
    }

    private synchronized Vote getCurrentVote() {
        return new Vote(this.proposedLeader, this.proposedEpoch);
    }

    public synchronized Vote lookForLeader()
    {
    	logger.fine("looking for leader");
        //send initial notifications to other peers to get things started
        sendNotifications();
        //Loop, exchanging notifications with other servers until we find a leader
        while (this.myPeerServer.getPeerState() == ServerState.LOOKING || this.isObserver) {
        	logger.fine("Current server is in "+this.myPeerServer.getPeerState()+" state");
            //Remove next notification from queue, timing out after 2 times the termination time
        	Message receivedMsg = recursiveBackoffNext(200, false);
        	if(receivedMsg == null) {
        		logger.fine("Recursive backing off did not bring any new messages... return null?");
        		return null;
        	} 
        	logger.fine("Recursive backing off received: " + receivedMsg.toString());
            //if/when we get a message and it's from a valid server and for a valid server..
        	//if any of the above are false, continue to next message
        	ElectionNotification receivedNotification = getNotificationFromMessage(receivedMsg);
        	if(this.myPeerServer.getPeerEpoch() > receivedNotification.getPeerEpoch()) {
        		logger.fine("Received message is invalid, continuing");
        		continue;
        	}
            //switch on the state of the sender:
        	logger.fine("Switch on state of sender: "+receivedNotification.getState());
        	switch(receivedNotification.getState()) {
                case LOOKING: //if the sender is also looking
            		//if the received message has a vote for a leader which supersedes mine, change my vote and tell all my peers what my new vote is.
            		if(!this.isObserver && supersedesCurrentVote(receivedNotification.getProposedLeaderID(), receivedNotification.getPeerEpoch())) {
            			logger.fine("Received vote superceedes current!\n"+
            					"changing current leader vote from ["+this.proposedLeader+"] to ["+receivedNotification.getProposedLeaderID()+"]\n"+
            					"and changing current epoch vote from ["+this.proposedEpoch+"] to ["+receivedNotification.getPeerEpoch()+"]");
            			this.proposedEpoch = receivedNotification.getPeerEpoch();
            			this.proposedLeader = receivedNotification.getProposedLeaderID();
            			sendNotifications();
            		}
            		//keep track of the votes I received and who I received them from.
            		ElectionNotification old = senderToNotification.put(receivedNotification.getSenderID(), receivedNotification);
            		if(old != null) {
            			logger.fine("Updated entry for server ID ["+receivedNotification.getSenderID()+"] from ["+old.toString()+"] to ["+receivedNotification.toString()+"]");
            		} else {
            			logger.fine("Updated entry for server ID ["+receivedNotification.getSenderID()+"] from [null] to ["+receivedNotification.toString()+"]");
            		}
            		////if I have enough votes to declare my currently proposed leader as the leader:
            		if(haveEnoughVotes(senderToNotification, this.getCurrentVote())) {
            			logger.fine("I have enough votes to declare a winner to my server. Wait some time for any last messages to come in");
            			try {
							Thread.sleep(finalizeWait);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
                    //first check if there are any new votes for a higher ranked possible leader before I declare a leader. If so, continue in my election loop
            			Set<Message> restOfMessages = new HashSet<>();
            			boolean continueElection = false;
            			logger.fine("Dumping all new messages");
            			while(incomingMessages.drainTo(restOfMessages) > 0) {
            				for(Message m : restOfMessages) {
            					ElectionNotification next = getNotificationFromMessage(m);
            					if(next.getState() == ServerState.OBSERVER) {
            						continue;
            					}
            					logger.fine("new message: [" + next.toString() + "]");
            					if(supersedesCurrentVote(next.getProposedLeaderID(), next.getPeerEpoch())) {
            						logger.fine("Superceedes current!!!");
            						continueElection = true;
            						break;
            					}
            					logger.fine("Does not superceed");
            				}
            			}
            			if(continueElection) {
            				logger.fine("Continuing election based off new message");
            				continue;
            			}
            			//If not, set my own state to either LEADING (if I won the election) or FOLLOWING (if someone else won the election) and exit the election
            			return acceptElectionWinner(receivedNotification);
            		}
            		break;
                case FOLLOWING: case LEADING: //if the sender is following a leader already or thinks it is the leader
                    //IF: see if the sender's vote allows me to reach a conclusion based on the election epoch that I'm in, i.e. it gives the majority to the vote of the FOLLOWING or LEADING peer whose vote I just received.
                	if(receivedNotification.getPeerEpoch() == this.proposedEpoch) {//still need to test if vote helps
                		logger.fine("received FOLLOWING or LEADING notification is in my epoch");
                		if(haveEnoughVotes(senderToNotification, receivedNotification)) {
                			logger.fine("received FOLLOWING or LEADING notification provides enough votes for a result");
                			return acceptElectionWinner(receivedNotification);
                		}
                		senderToNotification.put(receivedNotification.getSenderID(), receivedNotification);
                        //if so, accept the election winner.
                        //As, once someone declares a winner, we are done. We are not worried about / accounting for misbehaving peers.
                	}
                     //ELSE: if n is from a LATER election epoch
                	else if(receivedNotification.getPeerEpoch() > this.proposedEpoch){
                		logger.fine("Received FOLLOWING or LEADING notification is in an epoch greater than mine");
                        //IF a quorum from that epoch are voting for the same peer as the vote of the FOLLOWING or LEADING peer whose vote I just received.
                           //THEN accept their leader, and update my epoch to be their epoch
                		return this.acceptElectionWinner(receivedNotification);
                        //ELSE:
                            //keep looping on the election loop.
                	} else {
                		continue;
                	}
                	break;
                case OBSERVER:
                	continue;
        	}
        }
        Vote v = this.getCurrentVote();
        if(v == null) {
        	logger.fine("Server is not in LOOKING state. Returning null");
        } else {
        	logger.fine("Server is not in LOOKING state. Returning:" + v.toString());
        }
		return this.getCurrentVote();
    }
    
    public static byte[] buildMsgContent(ElectionNotification e) {
    	long leader = e.getProposedLeaderID();
    	char stateChar = e.getState().getChar();
    	long senderID = e.getSenderID();
    	long peerEpoch = e.getPeerEpoch();
    	ByteBuffer buffer = ByteBuffer.allocate(3*8 + 1 + 1);
    	buffer.putLong(leader);
    	buffer.putChar(stateChar);
    	buffer.putLong(senderID);
    	buffer.putLong(peerEpoch);
    	return buffer.array();
    }
    
    /**
     * Meant to send an ELECTION notification to everyone to start an election
     */
    private void sendNotifications() {
    	logger.fine("Sending notifications");
    	try {
    		this.myPeerServer.sendBroadcast(MessageType.ELECTION, deriveMessageContentFromCurrentState());
    	}
    	catch (Exception e) {
    		e.printStackTrace();
    		throw e;
    	}
    }
    
    /**
     * Derive messageContents byte[] based on current server state
     */
    private byte[] deriveMessageContentFromCurrentState() {
    	try {
    	long leader = this.proposedLeader;
    	char stateChar = this.myPeerServer.getPeerState().getChar();
    	long senderID = this.myPeerServer.getServerId();
    	long peerEpoch = this.proposedEpoch;
    	ByteBuffer buffer = ByteBuffer.allocate((3 * 8 + 1) + 1);
    	buffer.putLong(leader);
    	buffer.putChar(stateChar);
    	buffer.putLong(senderID);
    	buffer.putLong(peerEpoch);
    	return buffer.array();
    	}
    	catch(RuntimeException e) {
    		e.printStackTrace();
    		throw e;
    	}
    }
    
    /**
     * Convert Message to Vote
     * @param msg
     * @return
     */
    public static ElectionNotification getNotificationFromMessage(Message msg) {
    	if(msg == null) {
    		return null;
    	}
    	ByteBuffer msgBytes = ByteBuffer.wrap(msg.getMessageContents());
        long leader = msgBytes.getLong();
        char stateChar = msgBytes.getChar();
        long senderID = msgBytes.getLong();
        long peerEpoch = msgBytes.getLong();
        return new ElectionNotification(leader, ServerState.getServerState(stateChar), senderID, peerEpoch);
    }
    
    private Message recursiveBackoffNext(int waitTime, boolean sendNotifications) {
    	if(waitTime > maxNotificationInterval) {
    		logger.finest("requested waittime of "+waitTime+" MILLISECONDS is beyond wait time limit... return null");
    		return null;
    	}
    	logger.finest("Server waiting "+waitTime+" MILLISECONDS for next message");
    	try {
    	Message next = incomingMessages.poll(waitTime, TimeUnit.MILLISECONDS);
    	if(next == null) {
    		logger.finest("Server poll did not return a new Message...");
    		if(sendNotifications) {
    			sendNotifications();
    		}
    		return recursiveBackoffNext(waitTime *2, !sendNotifications);
    	} else {
    		return next;
    	}
    	} catch (InterruptedException e) {
    		throw new RuntimeException("Server was interupted while waiting for a new message to come in", e);
    	}
    }
     

    /**
     * Takes an election notification from a LOOKING server that tipped the scale to find a result 
     * @param n
     * @return
     */
    private Vote acceptElectionWinner(ElectionNotification n)
    {
    	logger.fine("Accepting an election winner from notification [" +n.toString()+"]");
    	this.incomingMessages.drainTo(new HashSet<Message>());
    	return new Vote(n.getProposedLeaderID(), n.getPeerEpoch());
    }

    /*
     * We return true if one of the following three cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     */
     protected boolean supersedesCurrentVote(long newId, long newEpoch) {
         return (newEpoch > this.proposedEpoch) || ((newEpoch == this.proposedEpoch) && (newId > this.proposedLeader));
     }
    /**
     * Termination predicate. Given a set of votes, determines if have sufficient support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one current vote
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification > votes, Vote proposal)
    {
    	HashMap<Long, Long> tally = new HashMap<>();
    	StringBuilder builder = new StringBuilder();
    	builder.append("\n");
    	builder.append("enter have enough votes check with proposal: " + proposal.myToString());
    	for(Long id : votes.keySet()) {
    		long proposedLeader = votes.get(id).getProposedLeaderID();
    		builder.append("id ["+id+"] votes ["+proposedLeader+"]\n");
    		if(tally.containsKey(proposedLeader)){
    			Long l = tally.get(proposedLeader);
    			tally.put(proposedLeader, l+1);
    		} else {
    			tally.put(proposedLeader, (long) 1);
    		}
    	}
    	long proposalCurrentCount = 0;
    	if(tally.containsKey(proposal.getProposedLeaderID())){
    			proposalCurrentCount = tally.get(proposal.getProposedLeaderID());
    	}
    	tally.put(proposal.getProposedLeaderID(), proposalCurrentCount + 1);
    	builder.append("incrementing on key "+proposal.getProposedLeaderID()+" from "+proposalCurrentCount+" to "+tally.get(proposal.getProposedLeaderID()+"\n"));
    	long highestVotedCount = 0, winningID = 0;
    	for(Long id : tally.keySet()) {
    		if(tally.get(id) > highestVotedCount) {
    			highestVotedCount = tally.get(id);
    			winningID = id;
    		}
    	}
    	builder.append("Winning vote is id ["+winningID+"] with ["+highestVotedCount+"] votes. Need ["+this.myPeerServer.getQuorumSize()+"] to make quorum\n");
    	boolean result = false;
    	//if(this.myPeerServer.getPeerState() != ServerState.OBSERVER) {
	    	builder.append("Proposed vote is for id ["+proposal.getProposedLeaderID()+"]\n");
	    	result = winningID == proposal.getProposedLeaderID() && highestVotedCount >= this.myPeerServer.getQuorumSize();
	    	builder.append("Does proposed leader win? "+ result + "\n");
    	/*} else {
    		result = highestVotedCount >= this.myPeerServer.getQuorumSize();
	    	builder.append("Does proposed leader win? "+ result + "\n");
    	}
    	*/
    	logger.fine(builder.toString());
    	return result;
    }
}
