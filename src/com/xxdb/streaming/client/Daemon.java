package com.xxdb.streaming.client;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

class Daemon  implements Runnable{
	private int listeningPort = 0;
	private MessageDispatcher dispatcher;
	private static final int KEEPALIVE_IDLE = 30000;
	private static final int KEEPALIVE_INTERVAL = 5000;
	private static final int KEEPALIVE_COUNT = 3;
	
	public Daemon(int port, MessageDispatcher dispatcher) {
		this.listeningPort = port;
		this.dispatcher = dispatcher;
	}

	@Override
	public void run() {
		ServerSocket ssocket = null;
		try {
			ssocket = new ServerSocket(this.listeningPort);
			while(true)
			{
				Socket socket = ssocket.accept();
				socket.setOOBInline(true);
				MessageParser listener = new MessageParser(socket, dispatcher);
				Thread listeningThread = new Thread(listener);
				listeningThread.start();
				new Thread(new ConnectionDetector(socket)).start();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally{
			if(ssocket!=null)
				try {
					ssocket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
	}
	
	class ConnectionDetector implements Runnable {
		Socket socket = null;
		public ConnectionDetector(Socket socket) {
			this.socket = socket;
		}

		@Override
		public void run() {
			while (true) {
				try {
					socket.sendUrgentData(0xFF);
				} catch (Exception ex) {
					int failCount = 0;
					for (int i = 0; i < KEEPALIVE_COUNT; i++) {
						try {
							socket.sendUrgentData(0xFF);
						} catch (Exception ex0) {
							failCount++;
						}
						
						try {
							Thread.sleep(KEEPALIVE_INTERVAL);
						} catch (Exception ex1) {
							ex.printStackTrace();
						}
					}
					if (failCount != KEEPALIVE_COUNT)
						continue;

					try {
						socket.close();
						return;
					} catch (Exception e) {
						return;
					}
				}

				try {
					Thread.sleep(KEEPALIVE_IDLE);
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		}
	}
}

