package udpgroupchat.server;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Map;

public class WorkerThread extends Thread {

	// number of times a packet will be resent if it times out
	private static final int MAX_TRIES = 3;

	// determines how long thread will wait to receive ack
	private static final long NUM_MILLIS_TILL_TIMEOUT = 10000;

	private DatagramPacket rxPacket;
	private DatagramSocket socket;

	public WorkerThread(DatagramPacket packet, DatagramSocket socket) {
		this.rxPacket = packet;
		this.socket = socket;
	}

	@Override
	public void run() {
		// convert the rxPacket's payload to a string
		String payload = new String(rxPacket.getData(), 0, rxPacket.getLength())
				.trim();

		// dispatch request handler functions based on the payload's prefix
		if (payload.startsWith("NAME")) {
			onNameRequest(payload);
			return;
		}

		// these requests all require ID's
		if (payload.startsWith("JOIN")) {
			onJoinRequest(payload);
			return;
		}

		if (payload.startsWith("MSG")) {
			onMSGRequest(payload);
			return;
		}
		
		if (payload.startsWith("POLL")) {
			onPOLLRequest(payload);
			return;
		}

		if (payload.startsWith("ACK")) {
			onACKRequest(payload);
			return;
		}
		
		if (payload.startsWith("SHUTDOWN") && this.rxPacket.getAddress().toString().equals("/127.0.0.1")) {
			onSHUTDOWNRequest(payload);
			return;
		}

		// if we got here, it must have been a bad request, so we tell the
		// client about it
		onBadRequest(payload);
	}

	// send a string, wrapped in a UDP packet, to the specified remote endpoint
	public void send(String payload, InetAddress address, int port)
			throws IOException {
		DatagramPacket txPacket = new DatagramPacket(payload.getBytes(),
				payload.length(), address, port);
		this.socket.send(txPacket);
	}

	private void onNameRequest(String payload) {
		String name = getRequestMessage("NAME", payload);

		// get the address of the sender from the rxPacket
		InetAddress address = this.rxPacket.getAddress();
		// get the port of the sender from the rxPacket
		int port = this.rxPacket.getPort();

		// generate a uniqueID
		int senderID = PubSubServer.generateID();
		
		// create a client object, and put it in the map that assigns ID's
		// to client objects
		PubSubServer.clientEndPoints.put(senderID, new ClientEndPoint(senderID, name, address, port));

		// send message, print error message, and remove client from clientEndPoint if ack not received
		if(!(sendAndAck(senderID, "NAMED " + name + " " + senderID + "\n"))) {
			PubSubServer.clientEndPoints.remove(senderID);
		}
	}

	private void onJoinRequest(String payload) {
		String reqMessage = getRequestMessage("JOIN", payload);
		String[] tokens = reqMessage.split(" ");
		int senderID = Integer.parseInt(tokens[0]);
		String groupName = tokens[1];

		// if sender doesn't have an ID (isn't registered)
		if (!(PubSubServer.clientEndPoints.containsKey(senderID))) {
			onBadRequest(payload);
			return;
		}
		
		updateClientInfo(senderID);

		// if group doesn't yet exist
		if (!(PubSubServer.groups.containsKey(groupName))) {
			// create new group, add to groups map
			PubSubServer.groups.put(groupName, new HashSet<Integer>());
		}
		// add this sender to the group
		PubSubServer.groups.get(groupName).add(senderID);

		// send message, and print error if ack not received
		sendAndAck(senderID, "JOINED " + groupName + "\n");
	}

	private void onMSGRequest(String payload) {
		String reqMessage = getRequestMessage("MSG", payload);
		String[] tokens = reqMessage.split(" ");
		int senderID = Integer.parseInt(tokens[0]);
		String groupName = tokens[1];
		
		// if sender doesn't have an ID (isn't registered)
		if (!(PubSubServer.clientEndPoints.containsKey(senderID))) {
			onBadRequest(payload);
			return;
		}
				
		updateClientInfo(senderID);
		
		StringBuilder builder = new StringBuilder();
		// the rest of the tokens are the message
		builder.append(tokens[2]);
		for (int i = 3; i < tokens.length; i++) {
			builder.append(" ");
			builder.append(tokens[i]);
		}
		String message = "From <"
				+ PubSubServer.clientEndPoints.get(senderID).name + "> To <"
				+ groupName + "> " + builder.toString() + "\n";
		
		for(Integer receiverID : PubSubServer.groups.get(groupName)) {
			// add the message to each group member's messageQueue
			if(receiverID != senderID) {
				PubSubServer.clientEndPoints.get(receiverID).messageQueue.add(message);
			}
		}
		sendAndAck(senderID, "MSG <" + builder.toString() + "> Sent to Group <" + groupName + ">\n");
	}

	private void onPOLLRequest(String payload) {
		String reqMessage = getRequestMessage("POLL", payload);
		int senderID = Integer.parseInt(reqMessage);
		
		// if sender doesn't have an ID (isn't registered)
		if (!(PubSubServer.clientEndPoints.containsKey(senderID))) {
			onBadRequest(payload);
			return;
		}
		
		updateClientInfo(senderID);
		
		while(!(PubSubServer.clientEndPoints.get(senderID).messageQueue.isEmpty())) {
			String nextMessage = PubSubServer.clientEndPoints.get(senderID).messageQueue.peek();
			sendAndAck(senderID, nextMessage);
			PubSubServer.clientEndPoints.get(senderID).messageQueue.poll();
		}
		sendAndAck(senderID, "EMPTY\n");
	}
	
	private void onACKRequest(String payload) {
		int senderID = Integer.parseInt(getRequestMessage("ACK", payload));
		// mark ack as received
		PubSubServer.acksReceived.put(senderID, true);
		// notify the waiting thread that the ack was received
		WorkerThread thread = PubSubServer.acks.get(senderID);
		synchronized (thread) {
			thread.notify();
		}
	}

	private void onSHUTDOWNRequest(String payload) {
		socket.close();
	}

	private void onBadRequest(String payload) {
		try {
			send("BAD REQUEST\n", this.rxPacket.getAddress(),
					this.rxPacket.getPort());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

	// helper methods

	private String getRequestMessage(String request, String payload) {
		return payload.substring(request.length() + 1, payload.length()).trim();
	}

	private boolean sendAndAck(int senderID, String message) {
		boolean ackReceived = false;
		for (int i = 0; i < MAX_TRIES; i++) {
			try {
				send(message, PubSubServer.clientEndPoints.get(senderID).address,
						PubSubServer.clientEndPoints.get(senderID).port);

				// add this thread to the collection of threads waiting on acks
				PubSubServer.acks.put(senderID, this);
				PubSubServer.acksReceived.put(senderID, false);

				synchronized (this) {
					try {
						this.wait(NUM_MILLIS_TILL_TIMEOUT);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				// if receive ack, break
				ackReceived = PubSubServer.acksReceived.get(senderID);
				if (ackReceived) {
					break;
				}
			} catch (InterruptedIOException e) { // acknowledgment timeout
				System.out.println("timed out");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (!(ackReceived)) {
			System.out.println("Client failed to acknowledge " + message);
		}
		return ackReceived;
	}
	
	// updates IP address and port of client in case it has changed since the last communication with the server
	private void updateClientInfo(int senderID) {
		PubSubServer.clientEndPoints.get(senderID).address = this.rxPacket.getAddress();
		PubSubServer.clientEndPoints.get(senderID).port = this.rxPacket.getPort();
	}
}
