package edu.yu.cs.com3800.HttpHandlers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl;

public class StateHandler implements HttpHandler{
	ZooKeeperPeerServerImpl server;
	
	public StateHandler(ZooKeeperPeerServerImpl server) {
		this.server = server;
	}

	@Override
	public void handle(HttpExchange exchange) throws IOException {
		InputStream is = exchange.getRequestBody();
		OutputStream os = exchange.getResponseBody();
		is.readAllBytes();//clear input stream
		String resp = this.server.getPeerState().toString();
		exchange.sendResponseHeaders(200, resp.getBytes().length);
		os.write(resp.getBytes());
		os.flush();
		exchange.close();
	}
}
