package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import com.sun.net.httpserver.HttpServer;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.ZooKeeperPeerServer;
import edu.yu.cs.com3800.HttpHandlers.ClientHandler;
import edu.yu.cs.com3800.HttpHandlers.GatewayClusterStatusHandler;

public class GatewayServer extends Thread implements LoggingServer{
	private final int myUdpPort;
	private final InetSocketAddress myHttpAddress;
    private Logger logger;
    HttpServer httpServer;
    private GatewayPeerServerImpl gatewayServer;
    
    public static void main(String[] args) throws IOException {
		if(args.length != 4) {
			throw new IllegalArgumentException("Gateway server requires 4 arguments to run!");
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
		GatewayServer gateway = new GatewayServer(epoch, id, map, set);
		gateway.start();
		try {
			Thread.sleep(Long.MAX_VALUE);
		} catch (InterruptedException e) {
			gateway.shutdown();
		}
	}
    
    /**
     * Sets up the Gateway server to listen on udp port of param myUdpPort, and the http port of myUdpPort + 1. Assumes port is included in the peerIDtoAddress map 
     * @param myUdpPort
     * @param id
     * @param peerIDtoAddress
     * @param peerEpoch
     * @param followerAddressSet
     * @throws IOException
     */
    public GatewayServer(long peerEpoch, Long id, HashMap<Long,InetSocketAddress> peerIDtoAddress, Set<InetSocketAddress> followerAddressSet) throws IOException {
    	//Do this early because the gateway peer server will remove its own address from the map provided to it!
    	HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
    	this.myUdpPort = peerIDtoAddress.get(id).getPort();
    	gatewayServer = new GatewayPeerServerImpl(this.myUdpPort + 3, peerEpoch, id, peerIDtoAddress, followerAddressSet); 
    	this.myHttpAddress = new InetSocketAddress("localhost", gatewayServer.getGatewayHttpPort());
    	this.logger = initializeLogging(this.getClass().getSimpleName() + "-on-udp-port-" + this.myUdpPort);
    	httpServer = HttpServer.create(this.myHttpAddress, 0);
    	httpServer.createContext(ZooKeeperPeerServer.COMPILEANDRUN, new ClientHandler(this.gatewayServer));
    	httpServer.createContext(ZooKeeperPeerServer.GATEWAY_CLUSTER_STATUS, new GatewayClusterStatusHandler(this.gatewayServer, map));
    	httpServer.setExecutor(null);
    }
    
    public GatewayServer(int port, long peerEpoch, Long id, HashMap<Long,InetSocketAddress> peerIDtoAddress, Set<InetSocketAddress> followerAddressSet) throws IOException {
    	this(peerEpoch, id, addToMap(port, id, peerIDtoAddress), followerAddressSet);
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
    
    public InetSocketAddress getHttpAddress() {
    	return this.myHttpAddress;
    }
    
    public void shutdown() {
    	this.gatewayServer.shutdown();
    	this.httpServer.stop(1);
    	interrupt();
    }
    
    @Override
    public void run() {
    	logger.fine("running gateway server and http server");
    	this.gatewayServer.start();
    	this.httpServer.start();
    	logger.fine("servers started");
    	while(true) {
    		logger.fine("Sleeping...");
    		try {
				Thread.sleep(Long.MAX_VALUE);
			} catch (InterruptedException e) {
				break;
			}
    	}
    }
    
    public GatewayPeerServerImpl getPeerServer() {
    	return this.gatewayServer;
    }
}
