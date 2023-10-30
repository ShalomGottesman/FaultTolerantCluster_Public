package edu.yu.cs.com3800;

import java.io.IOException;
import java.net.InetSocketAddress;

public interface ZooKeeperPeerServer extends LoggingServer{
	
	static final String LEADER_PATH = "/leader";
	static final String GOSSIP_PATH = "/gossipread";
	static final String GOSSIP_LOG_PATH = "/gossiplog";
	static final String SERVER_ROLE_PATH = "/serverrole";
	static final String COMPILEANDRUN = "/compileandrun";
	static final String GATEWAY_CLUSTER_STATUS = "/clusterstatus";
	

    void shutdown();
    
    boolean isShutdown();

    void setCurrentLeader(Vote v);

    Vote getCurrentLeader();

    void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException;

    void sendBroadcast(Message.MessageType type, byte[] messageContents);

    ServerState getPeerState();

    void setPeerState(ServerState newState);

    Long getServerId();

    long getPeerEpoch();

    InetSocketAddress getAddress();

    int getUdpPort();
    
    InetSocketAddress getPeerByID(long peerId);

    int getQuorumSize();

    void reportFailedPeer(long peerID);

    boolean isPeerDead(long peerID);

    boolean isPeerDead(InetSocketAddress address);
    
    /**
     * Only added method to this interface. GatewayPeerServerImpl needs to be able to override the zookeeper implementation as the gateway
     * also has the cluster status url
     * @throws IOException
     */
    public void startHttpServer() throws IOException;

    enum ServerState {
        LOOKING, FOLLOWING, LEADING, OBSERVER;

        public char getChar() {
            switch (this) {
                case LOOKING:
                    return 'O';
                case LEADING:
                    return 'E';
                case FOLLOWING:
                    return 'F';
                case OBSERVER:
                    return 'B';
            }
            return 'z';
        }

        public static ZooKeeperPeerServer.ServerState getServerState(char c) {
            switch (c) {
                case 'O':
                    return LOOKING;
                case 'E':
                    return LEADING;
                case 'F':
                    return FOLLOWING;
                case 'B':
                    return OBSERVER;
            }
            return null;
        }
    }

	int deadPeerCount();
}