package udpgroupchat.server;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class PubSubServer {

	// constants
	public static final int DEFAULT_PORT = 20000;
	public static final int MAX_PACKET_SIZE = 512;

	// port number to listen on
	protected int port;
	// map of clientEndPoints with ID keys
	// note that this is synchronized, i.e. safe to be read/written from
	// concurrent threads without additional locking
	protected static final Map<Integer, ClientEndPoint> clientEndPoints = Collections
			.synchronizedMap(new HashMap<Integer, ClientEndPoint>());
	// HashMap of groups
	protected static Map<String, Set<Integer>> groups = Collections
			.synchronizedMap(new HashMap<String, Set<Integer>>());
	// HashMap of pending ACKS
	protected static final Map<Integer, WorkerThread> acks = Collections
			.synchronizedMap(new HashMap<Integer, WorkerThread>());
	// HashMap of whether ACKS have been received
	protected static final Map<Integer, Boolean> acksReceived = Collections
			.synchronizedMap(new HashMap<Integer, Boolean>());
	// static int counter to use for room creation
	protected static AtomicInteger roomID = new AtomicInteger(0);
	// keeps track of whether or not there is a client currently waiting for another to join it's room
	protected static boolean isClientWaiting = false;
	// name of its group
	protected static String waitingClientGroup = "";
	// the waiting ClientEndPoint
	protected static ClientEndPoint waitingClient;

	
	// static int counter to use for ID's
	private static AtomicInteger IDCounter = new AtomicInteger(0);
	
	// constructor
	PubSubServer(int port) {
		this.port = port;
	}

	// start up the server
	public void start() {
		DatagramSocket socket = null;
		try {
			// create a datagram socket, bind to port port. See
			// http://docs.oracle.com/javase/tutorial/networking/datagrams/ for
			// details.

			socket = new DatagramSocket(port);

			// receive packets in an infinite loop
			while (true) {
				// create an empty UDP packet
				byte[] buf = new byte[PubSubServer.MAX_PACKET_SIZE];
				DatagramPacket packet = new DatagramPacket(buf, buf.length);
				// call receive (this will poulate the packet with the received
				// data, and the other endpoint's info)
				socket.receive(packet);
				System.out.println(new String(packet.getData(), 0, packet.getLength())
				.trim());
				// start up a worker thread to process the packet (and pass it
				// the socket, too, in case the
				// worker thread wants to respond)
				WorkerThread t = new WorkerThread(packet, socket);
				t.start();
			}
		} catch (IOException e) {
			// we jump out here if there's an error, or if the worker thread (or
			// someone else) closed the socket
			System.out.println("SHUTDOWN Complete");
		} finally {
			if (socket != null && !socket.isClosed())
				socket.close();
		}
	}
	
	// generates ID's for the clients
	public static int generateID() {
		return IDCounter.incrementAndGet();
	}

	// main method
	public static void main(String[] args) {
		int port = PubSubServer.DEFAULT_PORT;

		// check if port was given as a command line argument
		if (args.length > 0) {
			try {
				port = Integer.parseInt(args[0]);
			} catch (Exception e) {
				System.out.println("Invalid port specified: " + args[0]);
				System.out.println("Using default port " + port);
			}
		}

		// instantiate the server
		PubSubServer server = new PubSubServer(port);

		System.out
				.println("Starting server. Connect with netcat (nc -u 54.186.194.129 "
						+ port
						+ ") or start multiple instances of the client app to test the server's functionality.");

		// start it
		server.start();

	}

}
