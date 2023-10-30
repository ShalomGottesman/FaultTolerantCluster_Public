package edu.yu.cs.com3800.stage5;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.ZooKeeperPeerServer;
import edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState;
import edu.yu.cs.com3800.stage5.GatewayServer;
import edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl;

public class FailLeaderAndFollowerTest implements LoggingServer{
	static HashSet<ZooKeeperPeerServerImpl> serversToClose = new HashSet<>();
	static GatewayServer gatewayToShutdown = null;
	
	@AfterClass
	public static void closeServers() {
		for(ZooKeeperPeerServerImpl server : serversToClose) {
			server.shutdown();
		}
		if(gatewayToShutdown != null) {
			gatewayToShutdown.shutdown();
		}
	}
	
	@BeforeClass @AfterClass
	public static void resetProperty() {
		System.setProperty(LoggingServer.LOGGING_PREFIX, "");
	}
	@Test(timeout = 120000)
	public void failLeaderAndFollowerTest() throws InterruptedException, IOException {
		System.out.println("FailLeaderAndFollowerTest");
		Logger logger = initializeLogging(this.getClass().getSimpleName());
		System.setProperty(LoggingServer.LOGGING_PREFIX, "FailLeaderAndFollowerTest");
		HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(5);
        peerIDtoAddress.put(1L, new InetSocketAddress("localhost", 8310));
        peerIDtoAddress.put(2L, new InetSocketAddress("localhost", 8320));
        peerIDtoAddress.put(3L, new InetSocketAddress("localhost", 8330));
        peerIDtoAddress.put(4L, new InetSocketAddress("localhost", 8340));
        peerIDtoAddress.put(5L, new InetSocketAddress("localhost", 8350));
        peerIDtoAddress.put(6L, new InetSocketAddress("localhost", 8370));//OBSERVER server
        Long observerID = 6L;
        Set<InetSocketAddress> observerSet = new HashSet<InetSocketAddress>();
        observerSet.add(peerIDtoAddress.get(observerID));
        
        //initialize servers and get hooks on first election and first fail
        HashMap<Long, CountDownLatch> latchesToWaitForFails = new HashMap<>();
        HashMap<Long, CountDownLatch> latchToWaitForFirstElection = new HashMap<>();
        //non-observer servers
        HashMap<Long, ZooKeeperPeerServerImpl> servers = new HashMap<>();
        for(long x = observerID - 1; x > 0; x--) {
        	HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
            ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(0, x, map, observerSet);
            servers.put(x,server);
            latchesToWaitForFails.put(x, server.registerGeneralFailCount(2));
            latchToWaitForFirstElection.put(x, server.registerForServerStateChangeFrom(ServerState.LOOKING));
        }
        //observer server
        HashMap<Long, InetSocketAddress> gatewayMap = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
        GatewayServer gateway = new GatewayServer(0, observerID, gatewayMap, observerSet);
        latchesToWaitForFails.put(observerID, gateway.getPeerServer().registerGeneralFailCount(2));
        latchToWaitForFirstElection.put(observerID, gateway.getPeerServer().getLeaderChangeLatch());
        //start servers
        gateway.start();
        gatewayToShutdown = gateway;
        for(ZooKeeperPeerServerImpl server : servers.values()) {
        	new Thread((ZooKeeperPeerServerImpl) server, "Server on port " + server.getAddress().getPort()).start();
        	serversToClose.add(server);
        }
        //wait on latches for first election
        for(Long id : latchToWaitForFirstElection.keySet()) {
        	logger.fine("Waiting to leave first looking state in server: " + id +". Latch: "+latchToWaitForFirstElection.get(id).toString());
        	latchToWaitForFirstElection.get(id).await();
        	logger.fine("wait done");
        }
        //Verify that all servers have the same leaderID
        long leaderID = -1;
        for(ZooKeeperPeerServerImpl server : servers.values()) {
        	if(leaderID != -1) {
        		assertTrue("previous recorded leader is id :"+leaderID+" but for server "+server.getServerId()+" it was: "+server.getCurrentLeader().getProposedLeaderID(), leaderID == server.getCurrentLeader().getProposedLeaderID());
        	} else {
        		leaderID = server.getCurrentLeader().getProposedLeaderID();
        	}
        }
        assertTrue("Gateway leader id of: " + gateway.getPeerServer().getCurrentLeader().getProposedLeaderID() +" does not match leader id of: " +leaderID, leaderID == gateway.getPeerServer().getCurrentLeader().getProposedLeaderID());
        //before we shut down the leader, get hooks on next time servers (except leader) exit the LOOKING state
        HashMap<Long, CountDownLatch> stillInLookingState = new HashMap<>();
        for(ZooKeeperPeerServerImpl server : servers.values()) {
        	if(server.getServerId() == leaderID | server.getServerId() == 1L) {
        		continue;
        	}
    		CountDownLatch cdl = server.registerForServerStateChangeFrom(ServerState.LOOKING);
    		logger.fine("next looking latch for server "+server.getServerId()+" : "+cdl.toString());
    		stillInLookingState.put(server.getServerId(), cdl);
        }
        CountDownLatch cdl = gateway.getPeerServer().getLeaderChangeLatch();
        logger.fine("next looking latch for server "+gateway.getPeerServer().getServerId()+" : "+cdl.toString());
        stillInLookingState.put(gateway.getPeerServer().getServerId(), gateway.getPeerServer().getLeaderChangeLatch());
        
        //shut down leader and another
        servers.get(leaderID).shutdown();
        servers.get((Long)1L).shutdown(); 
        serversToClose.remove(servers.get(leaderID));
        serversToClose.remove(servers.get((Long)1L));
        logger.fine("called shutdown on server "+leaderID+" and 1. Wait on hooks");
        //wait for all other servers to detect the failure
        for(Long id : latchesToWaitForFails.keySet()) {
        	if(id == leaderID || id == 1L) {
        		continue;
        	}
        	logger.fine("Latch wait on id: " + id+". For latch: "+ latchesToWaitForFails.get(id).toString());
        	latchesToWaitForFails.get(id).await();
        	logger.fine("Latch for id " + id + " complete");
        }
        //wait for servers to exit LOOKING state
        logger.fine("wait for all servers to exit LOOKING");
        for(Long id : stillInLookingState.keySet()) {
        	logger.fine("Latch wait on id: " + id+". For latch: "+ stillInLookingState.get(id).toString());
        	stillInLookingState.get(id).await();
        	logger.fine("Latch for id " + id + " complete");
        }
        //verify that all servers have same new leader id
        long newLeaderID = -1;
        for(ZooKeeperPeerServerImpl server : servers.values()) {
        	if(server.getServerId() == leaderID || server.getServerId() == 1L) {
        		continue;
        	}
        	logger.fine("testing for new leader from server: " + server.getServerId());
        	assertNotNull("server with id: "+server.getServerId()+" has a null leader!", server.getCurrentLeader());
        	if(newLeaderID != -1) {
        		assertTrue("previous recorded leader is id :"+newLeaderID+" but for server "+server.getServerId()+" it was: "+server.getCurrentLeader().getProposedLeaderID(), newLeaderID == server.getCurrentLeader().getProposedLeaderID());
        	} else {
        		newLeaderID = server.getCurrentLeader().getProposedLeaderID();
        	}
        }
        assertNotNull("Gateway peer server current leader is null!", gateway.getPeerServer().getCurrentLeader());
        assertTrue("Gateway leader id of: " + gateway.getPeerServer().getCurrentLeader().getProposedLeaderID() +" does not match leader id of: " +newLeaderID, newLeaderID == gateway.getPeerServer().getCurrentLeader().getProposedLeaderID());
        for(ZooKeeperPeerServer server : servers.values()) {
        	if(!server.isShutdown()) {
        		server.shutdown();
        		serversToClose.remove(server);
        	}
        }
        gateway.shutdown();
        gatewayToShutdown = null;
	}

}
