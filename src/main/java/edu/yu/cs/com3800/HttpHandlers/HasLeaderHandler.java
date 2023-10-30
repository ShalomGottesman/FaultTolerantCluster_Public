package edu.yu.cs.com3800.HttpHandlers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import edu.yu.cs.com3800.Vote;
import edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl;

public class HasLeaderHandler implements HttpHandler{
	ZooKeeperPeerServerImpl server;
	
	public HasLeaderHandler(ZooKeeperPeerServerImpl server) {
		this.server = server;
	}

	@Override
	public void handle(HttpExchange exchange) throws IOException {
		InputStream is = exchange.getRequestBody();
		OutputStream os = exchange.getResponseBody();
		is.readAllBytes();//clear input stream
		Vote leader = this.server.getCurrentLeader();
		if(leader == null) {
			exchange.sendResponseHeaders(204, -1);
			os.flush();
			exchange.close();
		} else {
			String leaderID = leader.getProposedLeaderID() + "";
			exchange.sendResponseHeaders(200, leaderID.getBytes().length);
			os.write(leaderID.getBytes());
			os.flush();
			exchange.close();
		}
	}
}
