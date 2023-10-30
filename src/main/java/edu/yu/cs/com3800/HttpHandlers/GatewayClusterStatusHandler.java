package edu.yu.cs.com3800.HttpHandlers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import edu.yu.cs.com3800.ZooKeeperPeerServer;
import edu.yu.cs.com3800.stage1.Client.Response;
import edu.yu.cs.com3800.stage5.GatewayPeerServerImpl;

public class GatewayClusterStatusHandler implements HttpHandler{
	HashMap<Long, InetSocketAddress> id2peer;
	GatewayPeerServerImpl gateway;
	
	public GatewayClusterStatusHandler(GatewayPeerServerImpl gateway, HashMap<Long, InetSocketAddress> id2peer) {
		this.gateway = gateway;
		this.id2peer = id2peer;
	}

	@Override
	public void handle(HttpExchange exchange) throws IOException {
		//System.out.println("new request on cluster status!");
		InputStream is = exchange.getRequestBody();
		OutputStream os = exchange.getResponseBody();
		is.readAllBytes();//clear input stream
		StringBuilder builder = new StringBuilder();
		String format = "%3s|%30s|%11s|%11s|%13s|\n";
		builder.append(String.format(format, "ID", "UDP Address", "State", "Leader ID", "Marked Dead"));
		//System.out.printf(format, "ID", "UDP Address", "State", "Leader ID");
		Long[] orderedIDs = this.id2peer.keySet().toArray(new Long[0]);
		Arrays.sort(orderedIDs);
		Client[] stateClients = new Client[orderedIDs.length];
		Client[] leaderClients = new Client[orderedIDs.length];
		for(int y = 0; y < orderedIDs.length; y++) {
			long x = orderedIDs[y];
			if(x == gateway.getServerId()) {
				continue;
			} else {
				InetSocketAddress peer = this.id2peer.get(x);
				stateClients[y] = new Client(peer.getHostString(), peer.getPort() + 1, ZooKeeperPeerServer.SERVER_ROLE_PATH);
				leaderClients[y] = new Client(peer.getHostString(), peer.getPort() + 1, ZooKeeperPeerServer.LEADER_PATH);
				stateClients[y].start();
				leaderClients[y].start();
			}
		}
		for(int y = 0; y < orderedIDs.length; y++) {
			long x = orderedIDs[y];
			if(x == gateway.getServerId()) {
				InetSocketAddress peer = new InetSocketAddress("localhost", gateway.getUdpPort());
				try {
					builder.append(String.format(format, ""+gateway.getServerId(), peer.toString(), gateway.getPeerState(), ""+gateway.getCurrentLeader().getProposedLeaderID(), "false"));
					//System.out.printf(format, ""+gateway.getServerId(), peer.toString(), gateway.getPeerState(), ""+gateway.getCurrentLeader().getProposedLeaderID());
				} catch (Exception e) {
					builder.append(String.format(format, ""+x, peer.toString(), "Error", "Error", "false"));
					//System.out.printf(format, ""+x, peer.toString(), "Error", "Error");
				}	
			} else {
				InetSocketAddress peer = this.id2peer.get(x);
				try {
					stateClients[y].join();
					leaderClients[y].join();
				} catch (InterruptedException e) {
					
				}
				try {
					Response stateResp = stateClients[y].getResponse();
					Response leaderResp = leaderClients[y].getResponse();
					if(leaderResp.getCode() == 200) {
						builder.append(String.format(format, ""+x, peer.toString(), stateResp.getBody(), leaderResp.getBody(), ""+gateway.isPeerDead(peer)));
						//System.out.printf(format, ""+x, peer.toString(), stateResp.getBody(), leaderResp.getBody());
					} else {
						builder.append(String.format(format, ""+x, peer.toString(), stateResp.getBody(), "NONE", ""+gateway.isPeerDead(peer)));
						//System.out.printf(format, ""+x, peer.toString(), stateResp.getBody(), "NONE");
					}
				} catch (Exception e) {
					builder.append(String.format(format, ""+x, peer.toString(), "Error", "Error", ""+gateway.isPeerDead(peer)));
					//System.out.printf(format, ""+x, peer.toString(), "Error", "Error");
				}
			}
		}
		String resp = builder.toString();
		exchange.sendResponseHeaders(200, resp.getBytes().length);
		os.write(resp.getBytes());
		os.flush();
		exchange.close();
		
	}
	
	private class Client extends Thread{
		private int hostPort;
		private String hostName;
		private String context;
		private HttpClient client = HttpClient.newBuilder()
	            .version(HttpClient.Version.HTTP_2)
	            .connectTimeout(Duration.ofSeconds(8))
	            .build();
		HttpResponse<String> response;
		
		public Client(String hostName, int hostPort, String context) throws MalformedURLException{
			this.hostName = hostName;
			this.hostPort = hostPort;
			this.context = context;
		}
		
		@Override
		public void run() {
			String uri = "http://"+hostName+":"+hostPort+context;
			HttpRequest request = HttpRequest.newBuilder()
					.GET()
	                .uri(URI.create(uri))
	                .build();
			try {
				response = client.send(request, HttpResponse.BodyHandlers.ofString());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
			}
		}
		
		public Response getResponse() throws IOException {
			if(this.response == null) {
				return null;
			} else {
				return new Response(this.response.statusCode(), this.response.body());
			}
		}
	}

}
