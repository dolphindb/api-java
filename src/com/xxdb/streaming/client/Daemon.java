package com.xxdb.streaming.client;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

class Daemon  implements Runnable{
	private int listeningPort = 0;
	private MessageDispatcher dispatcher;
	private static final int KEEPALIVE_IDLE = 1000;
	private static final int KEEPALIVE_INTERVAL = 1000;
	private static final int KEEPALIVE_COUNT = 5;
	
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
		new Thread(new ReconnectDetector(dispatcher)).start();

		while(true)	{
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
					ex1.printStackTrace();
				}
			}catch (Throwable t){
				try {
					Thread.sleep(100);
				}catch (Exception ex1){
					ex1.printStackTrace();
				}
			}
		}
	}
	
	class ReconnectDetector implements Runnable {
		MessageDispatcher dispatcher = null;
		public ReconnectDetector(MessageDispatcher d) {
			this.dispatcher = d;
		}
		@Override
		public void run() {
			while(true){
				try {
					for (String site : this.dispatcher.getAllReconnectSites()) {
						if (dispatcher.getNeedReconnect(site) == 1) {
								AbstractClient.Site s = dispatcher.getSiteByName(site);
								dispatcher.activeCloseConnection(s);
								String lastTopic = "";
								for(String topic : dispatcher.getAllTopicsBySite(site)) {
									System.out.println("try to reconnect topic " + topic);
									dispatcher.tryReconnect(topic);
									lastTopic = topic;
								}
								dispatcher.setNeedReconnect(lastTopic, 2);
							} else {
								// try reconnect after 3 second when reconnecting stat
								long ts = dispatcher.getReconnectTimestamp(site);
								if (System.currentTimeMillis() >= ts + 3000) {
									AbstractClient.Site s = dispatcher.getSiteByName(site);
									dispatcher.activeCloseConnection(s);
									for(String topic : dispatcher.getAllTopicsBySite(site)) {
										System.out.println("try to reconnect topic " + topic);
										dispatcher.tryReconnect(topic);
									}
									dispatcher.setReconnectTimestamp(site, System.currentTimeMillis());
								}
							}
						}
						for(String topic : dispatcher.getAllReconnectTopic()){
							dispatcher.tryReconnect(topic);
						}
				}catch(Exception e0){
					e0.printStackTrace();
				}
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
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
						System.out.println("Connection closed!!");
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

