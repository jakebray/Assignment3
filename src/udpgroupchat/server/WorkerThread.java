package udpgroupchat.server;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.HashSet;

public class WorkerThread extends Thread {

	// number of times a packet will be resent if it times out
	private static final int MAX_TRIES = 5;

	// determines how long thread will wait to receive ack
	private static final long NUM_MILLIS_TILL_TIMEOUT = 5000;

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
			onJOINRequest(payload);
			return;
		}
		
		if (payload.startsWith("FIND")) {
			onFINDRequest(payload);
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

		if (payload.startsWith("QUIT")) {
			onQUITRequest(payload);
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

	private void onJOINRequest(String payload) {
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
	
	private void onFINDRequest(String payload) {
		int senderID = Integer.parseInt(getRequestMessage("FIND", payload));

		// if sender doesn't have an ID (isn't registered)
		if (!(PubSubServer.clientEndPoints.containsKey(senderID))) {
			onBadRequest(payload);
			return;
		}
		
		updateClientInfo(senderID);

		// this getRoom() method will make the appropriate sendAndAcks
		String groupName = getRoom(senderID);
		
		// if group doesn't yet exist
		if (!(PubSubServer.groups.containsKey(groupName))) {
			// create new group, add to groups map
			PubSubServer.groups.put(groupName, new HashSet<Integer>());
		}
		// add this sender to the group
		PubSubServer.groups.get(groupName).add(senderID);
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
		String message = "From "
				+ PubSubServer.clientEndPoints.get(senderID).name + " To "
				+ groupName + " " + builder.toString() + "\n";
		
		for(Integer receiverID : PubSubServer.groups.get(groupName)) {
			// add the message to each group member's messageQueue
			if(receiverID != senderID) {
				PubSubServer.clientEndPoints.get(receiverID).messageQueue.add(message);
			}
		}
		sendAndAck(senderID, "MSG " + builder.toString() + " Sent to Group " + groupName + ">\n");
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
			if(sendAndAck(senderID, nextMessage)) {
				PubSubServer.clientEndPoints.get(senderID).messageQueue.poll();
			} else {
				// client must be temporarily disconnected
				return;
			}
		}
		sendAndAck(senderID, "EMPTY\n");
	}
	
	private void onQUITRequest(String payload) {
		String reqMessage = getRequestMessage("QUIT", payload);
		String[] tokens = reqMessage.split(" ");
		int senderID = Integer.parseInt(tokens[0]);
		String groupName = tokens[1];

		// if sender doesn't have an ID (isn't registered)
		if (!(PubSubServer.clientEndPoints.containsKey(senderID))) {
			onBadRequest(payload);
			return;
		}
		
		updateClientInfo(senderID);
		
		// remove this sender from the group
		PubSubServer.groups.get(groupName).remove(senderID);
		// if this makes the group empty, then remove it
		if (PubSubServer.groups.get(groupName).isEmpty()) {
			PubSubServer.groups.remove(groupName);
		}

		// send message, and print error if ack not received
		sendAndAck(senderID, "QUIT Group " + groupName + "\n");
	}
	
	private void onACKRequest(String payload) {
		int senderID = Integer.parseInt(getRequestMessage("ACK", payload));
		// mark ack as received
		if (PubSubServer.acks.containsKey(senderID)) {
			PubSubServer.acksReceived.put(senderID, true);
			// notify the waiting thread that the ack was received
			WorkerThread thread = PubSubServer.acks.get(senderID);
			synchronized (thread) {
				thread.notify();
			}
		}
	}

	private void onSHUTDOWNRequest(String payload) {
		int senderID = Integer.parseInt(getRequestMessage("SHUTDOWN", payload));
		sendAndAck(senderID, "SHUTTING DOWN\n");
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

	// keeps track of clients waiting for another to join room
	private String getRoom(int senderID) {
		synchronized (PubSubServer.waitingClientGroup) {
			String roomName;
			// if there is currently a waiting client, then match this client to it, notify the waiter, and remove it from the wait list
			if (PubSubServer.isClientWaiting) {
				// note: hashCode() returns the CLient's ID
				// if waiter and sender both accept FOUND messages, then pair them up
				String foundMsg = "FOUND " + PubSubServer.waitingClientGroup + "\n";
				if (sendAndAck(PubSubServer.waitingClient.hashCode(), foundMsg) && sendAndAck(senderID, foundMsg)) {
					PubSubServer.isClientWaiting = false;
					roomName = PubSubServer.waitingClientGroup;
				} else {
					// if no response from waiter, remove the waiter from the group
					PubSubServer.groups.get(PubSubServer.waitingClientGroup).remove(senderID);
					// if this makes the group empty, then remove it
					if (PubSubServer.groups.get(PubSubServer.waitingClientGroup).isEmpty()) {
						PubSubServer.groups.remove(PubSubServer.waitingClientGroup);
					}
					// note: calls sendAndAck
					roomName = genRoom(senderID);
				}
			} else {
				// note: calls sendAndAck
				roomName = genRoom(senderID);
			}
			return roomName;
		}
	}
	
	// keeps track of clients waiting for another to join room
	private String genRoom(int senderID) {
		synchronized (PubSubServer.waitingClientGroup) {
			// generate a string in the form "roomX", where X is a unique number
			// then update variables to add this room to the wait list
			String roomName = "room" + PubSubServer.roomID.incrementAndGet();
			PubSubServer.isClientWaiting = true;
			PubSubServer.waitingClientGroup = roomName;
			PubSubServer.waitingClient = PubSubServer.clientEndPoints.get(senderID);
			sendAndAck(senderID, "WAITING\n");
			return roomName;
		}
	}

	private boolean sendAndAck(int senderID, String message) {
		boolean ackReceived = false;
		
		// add this thread to the collection of threads waiting on acks
		PubSubServer.acks.put(senderID, this);
		PubSubServer.acksReceived.put(senderID, false);
		
		for (int i = 0; i < MAX_TRIES; i++) {
			try {
				send(message, PubSubServer.clientEndPoints.get(senderID).address,
						PubSubServer.clientEndPoints.get(senderID).port);

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
		// remove this thread from the collection of threads waiting on acks
		PubSubServer.acks.remove(senderID);
		PubSubServer.acksReceived.remove(senderID);
		
		// return whether or not the ack was ever received
		return ackReceived;
	}
	
	// updates IP address and port of client in case it has changed since the last communication with the server
	private void updateClientInfo(int senderID) {
		PubSubServer.clientEndPoints.get(senderID).address = this.rxPacket.getAddress();
		PubSubServer.clientEndPoints.get(senderID).port = this.rxPacket.getPort();
	}
}
