package udpgroupchat.server;

import java.net.InetAddress;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ClientEndPoint {
	private int ID;
	protected String name;
	protected InetAddress address;
	protected int port;
	protected Queue<String> messageQueue;
	
	public ClientEndPoint(int ID, String name, InetAddress addr, int port) {
		this.ID = ID;
		this.name = name;
		this.address = addr;
		this.port = port;
		this.messageQueue = new ConcurrentLinkedQueue<String>();
	}
	
	// used if client ip changes
	public void setAddressAndPort(InetAddress addr, int port) {
		this.address = addr;
		this.port = port;
	}

	@Override
	public int hashCode() {
		return this.ID;
	}
	
	@Override
	public boolean equals(Object other) {
		// two endpoints are considered equal if their hash codes are equal
		return this.hashCode() == other.hashCode();
	}
	
}
