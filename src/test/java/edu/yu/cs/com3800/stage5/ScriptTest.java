package edu.yu.cs.com3800.stage5;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.ZooKeeperPeerServer;
import edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState;
import edu.yu.cs.com3800.stage1.ClientImpl;

public class ScriptTest implements LoggingServer{
	private String validClass = "public class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
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
	
	@Test(timeout = 240000)
	public void scriptTest() throws InterruptedException, IOException, ExecutionException {
		System.out.println("ScriptTest");
		Logger logger = initializeLogging(this.getClass().getSimpleName());
		System.setProperty(LoggingServer.LOGGING_PREFIX, "ScriptTest");
		HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(5);
        peerIDtoAddress.put(1L, new InetSocketAddress("localhost", 8410));
        peerIDtoAddress.put(2L, new InetSocketAddress("localhost", 8420));
        peerIDtoAddress.put(3L, new InetSocketAddress("localhost", 8430));
        peerIDtoAddress.put(4L, new InetSocketAddress("localhost", 8440));
        peerIDtoAddress.put(5L, new InetSocketAddress("localhost", 8450));
        peerIDtoAddress.put(6L, new InetSocketAddress("localhost", 8460));
        peerIDtoAddress.put(7L, new InetSocketAddress("localhost", 8470));
        peerIDtoAddress.put(8L, new InetSocketAddress("localhost", 8480));
        Long observerID = 8L;
        Set<InetSocketAddress> observerSet = new HashSet<InetSocketAddress>();
        observerSet.add(peerIDtoAddress.get(observerID));
        
        //initialize servers and get hooks on first election and first fail
        HashMap<Long, CountDownLatch> latchesToWaitForFails = new HashMap<>();
        HashMap<Long, CountDownLatch> latchToWaitForFirstElection = new HashMap<>();
        //non-observer servers
        HashMap<Long, ZooKeeperPeerServerImpl> servers = new HashMap<>();
        for(long x = observerID - 1; x > 0; x--) {
        	@SuppressWarnings("unchecked")
			HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
            ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(0, x, map, observerSet);
            servers.put(x,server);
            latchesToWaitForFails.put(x, server.registerGeneralFailCount(1));
            latchToWaitForFirstElection.put(x, server.registerForServerStateChangeFrom(ServerState.LOOKING));
        }
        //observer server
        @SuppressWarnings("unchecked")
		HashMap<Long, InetSocketAddress> gatewayMap = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
        GatewayServer gateway = new GatewayServer(0, observerID, gatewayMap, observerSet);
        latchesToWaitForFails.put(observerID, gateway.getPeerServer().registerGeneralFailCount(1));
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
      //initialize clients and begin execution. Large number so we can capture when clients come in during leader fail
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 2, Runtime.getRuntime().availableProcessors() * 2, 100, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        Client[] clients = new Client[9];
        @SuppressWarnings("rawtypes")
		Future[] futs = new Future[clients.length];
        for(int x = 0; x < clients.length; x++) {
        	String code = this.validClass.replace("Hello world!", ""+0);
        	clients[x] = new Client("localhost", gateway.getHttpAddress().getPort(), code);
        	futs[x] = tpe.submit(clients[x]);
        	logger.fine("Client "+x+" created with code ["+code+"]");
        }
        for(int x = 0; x < clients.length; x++) {
        	futs[x].get();
        	assertTrue("Client "+x+" response value is"+clients[x].resp, clients[x].resp.equals("" + 0));
        	//TODO should print request and response here
        }

        //before we shut down the leader and Client, get hooks on next time servers (except leader) exit the LOOKING state
        //also get hooks on when server with ID 1 is shut down
        HashMap<Long, CountDownLatch> stillInLookingState = new HashMap<>();
        HashMap<Long, CountDownLatch> server1DiscoveredDead = new HashMap<>();
        for(ZooKeeperPeerServerImpl server : servers.values()) {
        	if(server.getServerId() != leaderID) {
	    		CountDownLatch cdl = server.registerForServerStateChangeFrom(ServerState.LOOKING);
	    		logger.fine("next looking latch for server "+server.getServerId()+" : "+cdl.toString());
	    		stillInLookingState.put(server.getServerId(), cdl);
        	}
        	if(server.getServerId() != 1) {
        		CountDownLatch cdl = server.registerForServerFail(1L);
        		logger.fine("next latch for server to discover server[1] fail for server "+server.getServerId()+" : "+cdl.toString());
        		server1DiscoveredDead.put(server.getServerId(), cdl);
        	}
        }
        CountDownLatch cdl = gateway.getPeerServer().getLeaderChangeLatch();
		logger.fine("next looking latch for server "+gateway.getPeerServer().getServerId()+" : "+cdl.toString());
        stillInLookingState.put(gateway.getPeerServer().getServerId(), gateway.getPeerServer().getLeaderChangeLatch());
        
        //shut down a follower
        Long serverToKill = 1L;
        servers.get(serverToKill).shutdown();
        serversToClose.remove(servers.get(serverToKill));
        //remove latch to wait for discovery of failed Leader
        latchesToWaitForFails.remove(serverToKill);
        stillInLookingState.remove(serverToKill);
        
        //Wait for discovery
        for(Long id : server1DiscoveredDead.keySet()) {
        	logger.fine("Latch wait on id: " + id+". For latch: "+ server1DiscoveredDead.get(id).toString());
        	latchesToWaitForFails.get(id).await();
        	logger.fine("Latch for id " + id + " complete");
        }
        //shut down leader server
        servers.get(leaderID).shutdown();
        serversToClose.remove(servers.get(leaderID));
        logger.fine("called shutdown on server "+leaderID+". Wait on hooks");
        //wait for all other servers to detect the failure
        for(Long id : latchesToWaitForFails.keySet()) {
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
        	if(server.getServerId() == leaderID || server.getServerId() == serverToKill) {
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
        for(int x = 0; x < clients.length; x++) {
        	String code = this.validClass.replace("Hello world!", ""+0);
        	clients[x] = new Client("localhost", gateway.getHttpAddress().getPort(), code);
        	futs[x] = tpe.submit(clients[x]);
        	logger.fine("Client "+x+" created with code ["+code+"]");
        }
        for(int x = 0; x < clients.length; x++) {
        	futs[x].get();
        	assertTrue("Client "+x+" response value is"+clients[x].resp, clients[x].resp.equals("" + 0));
        	//TODO should print request and response here
        }
        tpe.shutdown();
        //cleanup
        for(ZooKeeperPeerServer server : servers.values()) {
        	if(!server.isShutdown()) {
        		server.shutdown();
        		serversToClose.remove(server);
        	}
        }
        gateway.shutdown();
        gatewayToShutdown = null;
	}
	
	private class Client extends Thread{
		ClientImpl myClient;
		String code;
		String resp = "";
		
		public Client(String host, int port, String src) throws MalformedURLException {
			this.myClient = new ClientImpl(host, port);
			this.code = src;
		}

		@Override
		public void run() {
			try {
				myClient.sendCompileAndRunRequest(code);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				this.resp = myClient.getResponse().getBody();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
