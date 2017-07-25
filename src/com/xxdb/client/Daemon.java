package com.xxdb.client;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Daemon  implements Runnable{
	
	private int listeningPort = 0;
	private QueueManager queueManager;
	public Daemon(int port, QueueManager queueManager) {
		this.listeningPort = port;
		this.queueManager = queueManager;
	}

	@Override
	public void run() {
		ServerSocket ssocket = null;
		try {
			ssocket = new ServerSocket(this.listeningPort);
			while(true)
			{
				Socket socket = ssocket.accept();
				MessageQueueParser listener = new MessageQueueParser(socket, queueManager);
				Thread listeningThread = new Thread(listener);
				listeningThread.start();
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
	
	
	
}

