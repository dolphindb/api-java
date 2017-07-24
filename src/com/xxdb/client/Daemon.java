package com.xxdb.client;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Daemon  implements Runnable{
	
	private int _listeningPort = 0;
	private QueueManager _queueManager;
	public Daemon(int port, QueueManager queueManager) {
		this._listeningPort = port;
		this._queueManager = queueManager;
	}

	@Override
	public void run() {
		
		try {
			ServerSocket ssocket = new ServerSocket(this._listeningPort);
			while(true)
			{
				Socket socket = ssocket.accept();
				MessageQueueParser listener = new MessageQueueParser(socket, _queueManager);
				Thread listeningThread = new Thread(listener);
				listeningThread.start();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	
	
}

