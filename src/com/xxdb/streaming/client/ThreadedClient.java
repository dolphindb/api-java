package com.xxdb.streaming.client;

import com.xxdb.streaming.client.IMessage;

import java.io.IOException;
import java.net.SocketException;
import java.util.List;
import java.util.concurrent.BlockingQueue;


public class ThreadedClient extends  AbstractClient {
	private HandlerLopper handlerLopper = null;

	public ThreadedClient() throws SocketException {
        this(DEFAULT_PORT);
    }
	
    public ThreadedClient(int subscribePort) throws SocketException{
        super(subscribePort);
    }
    
    class HandlerLopper extends Thread{
        BlockingQueue<List<IMessage>> queue;
        MessageHandler handler;
        HandlerLopper(BlockingQueue<List<IMessage>> queue, MessageHandler handler) {
            this.queue = queue;
            this.handler = handler;
        }
        public void run() {
            while(true) {
                try {
                    List<IMessage> msgs = queue.take();
                    for (IMessage msg : msgs) {
                        handler.doEvent(msg);
                    }
                } catch (InterruptedException e) {
                    System.out.println("Handler thread stopped.");
                }
            }
        }
    }
	
	@Override
	protected void doReconnect(Site site) {
		if (handlerLopper == null)
			throw new RuntimeException("Subscribe thread is not started");
		handlerLopper.interrupt();
		while (true) {
			try {
				subscribe(site.host, site.port, site.tableName, site.actionName, site.handler, site.msgId + 1, true);
				System.out.println("Successfully reconnected and subscribed " + site.host + ":" + site.port + ":" + site.tableName);
				return;
			} catch (Exception ex) {
				System.out.println("Unable to subscribe table. Will try again after 5 seconds.");
				ex.printStackTrace();
				try {
					Thread.sleep(5000);
				} catch (Exception ex0){
					
				}
			}
		}
	}

    public void subscribe(String host,int port,String tableName,String actionName,MessageHandler handler,long offset,boolean reconnect) throws IOException {
        BlockingQueue<List<IMessage>> queue = subscribeInternal(host,port,tableName,actionName,handler,offset,reconnect);
        handlerLopper = new HandlerLopper(queue, handler);
        handlerLopper.start();
    }

    public void subscribe(String host,int port,String tableName,String actionName,MessageHandler handler,long offset) throws IOException {
		subscribe(host, port, tableName, actionName, handler, offset, false);
	}

    public void subscribe(String host,int port,String tableName,String actionName,MessageHandler handler) throws IOException {
        subscribe(host, port, tableName,actionName, handler, -1);
    }
    
    public void subscribe(String host,int port,String tableName,String actionName,MessageHandler handler,boolean reconnect) throws IOException {
        subscribe(host, port, tableName,actionName, handler, -1, reconnect);
    }
    
	public void subscribe(String host,int port,String tableName,MessageHandler handler) throws IOException {
        subscribe(host, port, tableName,DEFAULT_ACTION_NAME, handler, -1);
    }
	
	public void subscribe(String host,int port,String tableName,MessageHandler handler,boolean reconnect) throws IOException {
        subscribe(host, port, tableName,DEFAULT_ACTION_NAME, handler, -1, reconnect);
    }
	
	public void subscribe(String host,int port,String tableName,MessageHandler handler,long offset) throws IOException {
        subscribe(host, port, tableName,DEFAULT_ACTION_NAME, handler, offset);
    }
	
	public void subscribe(String host,int port,String tableName,MessageHandler handler,long offset,boolean reconnect) throws IOException {
        subscribe(host, port, tableName,DEFAULT_ACTION_NAME, handler, offset, reconnect);
    }

    public void unsubscribe(String host,int port ,String tableName, String actionName) throws IOException {
        unsubscribeInternal(host, port, tableName, actionName);
    }

    public void unsubscribe(String host,int port ,String tableName) throws IOException {
        unsubscribeInternal(host, port, tableName,"");
    }
}
