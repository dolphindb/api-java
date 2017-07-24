package com.xxdb.consumer;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Daemon  implements Runnable{
	
	private int _listeningPort = 0;
	
	public Daemon(int port) {
		this._listeningPort = port;
	}

	@Override
	public void run() {
		
		try {
			ServerSocket ssocket = new ServerSocket(this._listeningPort);
			while(true)
			{
				Socket socket = ssocket.accept();
				MessageQueueParser listener = new MessageQueueParser(socket);
				Thread listeningThread = new Thread(listener);
				listeningThread.start();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	
	
}

