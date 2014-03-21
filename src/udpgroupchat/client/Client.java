package udpgroupchat.client;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;

import udpgroupchat.server.PubSubServer;

public class Client {

	String serverAddress;
	int serverPort;
	DatagramSocket socket;
	InetSocketAddress serverSocketAddress;

	// constructor
	Client(String serverAddress, int serverPort) {
		this.serverAddress = serverAddress;
		this.serverPort = serverPort;
		serverSocketAddress = new InetSocketAddress(serverAddress, serverPort);
	}

	// start up the server
	public void start() {
		try {
			socket = new DatagramSocket();
			int jackID, jillID;
			
			System.out.println("Jack registering: ");
			jackID = sendAndReceive(0, "NAME Jack");
			System.out.println("\nJill registering: ");
			jillID = sendAndReceive(0, "NAME Jill");
			
			String stringJackID = " " + jackID + " ";
			String stringJillID = " " + jillID + " ";
			
			System.out.println("\nJack joining a group: ");
			sendAndReceive(jackID, "JOIN" + stringJackID + "ChatRoom");
			System.out.println("\nJill joining that group: ");
			sendAndReceive(jillID, "JOIN" + stringJillID + "ChatRoom");
			
			System.out.println("\nJack sending messages to the group: ");
			sendAndReceive(jackID, "MSG" + stringJackID + "ChatRoom Hello, welcome to ChatRoom, friends!");
			sendAndReceive(jackID, "MSG" + stringJackID + "ChatRoom My name is Jack, nice to meet you!");
			sendAndReceive(jackID, "MSG" + stringJackID + "ChatRoom What's your name?");

			System.out.println("\nJill receiving messages, and sending one of her own: ");
			pollAndReceive(jillID, "POLL" + stringJillID);
			sendAndReceive(jillID, "MSG" + stringJillID + "ChatRoom My name is Jill");
			
			System.out.println("\nJack receiving messages: ");
			pollAndReceive(jackID, "POLL" + stringJackID);
			
			sendAndReceive(jackID, "SHUTDOWN");
			
		} catch (IOException e) {
			// we jump out here if there's an error
			e.printStackTrace();
		} finally {
			// close the socket
			if(socket!=null && !socket.isClosed())
				socket.close();
		}
	}
	
	// helper methods
	int sendAndReceive (int ID, String message) throws IOException {
		byte[] buf = new byte[PubSubServer.MAX_PACKET_SIZE];
		DatagramPacket rxPacket = new DatagramPacket(buf, buf.length);
		
		DatagramPacket txPacket = new DatagramPacket(message.getBytes(), message.length(),
				serverSocketAddress);
		
		socket.send(txPacket);
		socket.receive(rxPacket);
		String str = new String(rxPacket.getData(), 0, rxPacket.getLength())
		.trim();
		String[] strings = str.split(" ");
		
		int senderID;
		if(ID == 0) {
			senderID = Integer.parseInt(strings[strings.length - 1]);
		} else {
			senderID = ID;
		}
		
		// print the payload
		System.out.println(str);
		
		message = "ACK " + senderID;
		txPacket = new DatagramPacket(message.getBytes(), message.length(),
				serverSocketAddress);
		socket.send(txPacket);
		return senderID;
	}
	
	void pollAndReceive (int ID, String message) throws IOException {
		while(true) {
			byte[] buf = new byte[PubSubServer.MAX_PACKET_SIZE];
			DatagramPacket rxPacket = new DatagramPacket(buf, buf.length);
			
			DatagramPacket txPacket = new DatagramPacket(message.getBytes(), message.length(),
					serverSocketAddress);
			
			socket.send(txPacket);
			socket.receive(rxPacket);
			String str = new String(rxPacket.getData(), 0, rxPacket.getLength())
			.trim();
			String[] strings = str.split(" ");
			
			int senderID;
			if(ID == 0) {
				senderID = Integer.parseInt(strings[strings.length - 1]);
			} else {
				senderID = ID;
			}
			
			// print the payload
			if(str.equals("EMPTY")) {
				break;
			}
			System.out.println(str);
			
			message = "ACK " + senderID;
			txPacket = new DatagramPacket(message.getBytes(), message.length(),
					serverSocketAddress);
			socket.send(txPacket);
		}
	}

	// main method
	public static void main(String[] args) {
		int serverPort = PubSubServer.DEFAULT_PORT;
		String serverAddress = "localhost";

		// check if server address and port were given as command line arguments
		if (args.length > 0) {
			serverAddress = args[0];
		}

		if (args.length > 1) {
			try {
				serverPort = Integer.parseInt(args[1]);
			} catch (Exception e) {
				System.out.println("Invalid serverPort specified: " + args[0]);
				System.out.println("Using default serverPort " + serverPort);
			}
		}

		// instantiate the client
		Client client = new Client(serverAddress, serverPort);

		// start it
		client.start();
	}

}
