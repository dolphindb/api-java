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
		}catch (IOException e) {
			e.printStackTrace();
		}
		while(true)
		{
			try {
				Socket socket = ssocket.accept();
				socket.setKeepAlive(true);
				MessageParser listener = new MessageParser(socket, dispatcher);
				Thread listeningThread = new Thread(listener);
				listeningThread.start();
				if (!System.getProperty("os.name").equalsIgnoreCase("linux"))
					new Thread(new ConnectionDetector(socket)).start();
			}catch (Exception ex){
				try {
					Thread.sleep(100);
				}catch (Exception ex1){

				}
			}catch (Throwable t){
				try {
					Thread.sleep(100);
				}catch (Exception ex1){

				}
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
						System.out.println("send Urgent Data !!!");
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

