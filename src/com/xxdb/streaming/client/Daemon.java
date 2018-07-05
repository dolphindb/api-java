package com.xxdb.streaming.client;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

class Daemon  implements Runnable{
	private int listeningPort = 0;
	private MessageDispatcher dispatcher;
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
				MessageParser listener = new MessageParser(socket, dispatcher);
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

