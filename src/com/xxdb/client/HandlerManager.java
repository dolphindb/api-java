package com.xxdb.client;
import java.util.HashMap;
import com.xxdb.client.datatransferobject.IMessage;

public class HandlerManager {
	private HashMap<String, IncomingMessageHandler> handlers;

    public void addIncomingMessageHandler(String topic, IncomingMessageHandler listener) {
        if (handlers == null) {
            handlers = new HashMap<String,IncomingMessageHandler>();
        }
        handlers.put(topic,listener);
    }

    public IncomingMessageHandler removeIncomingMessageHandler(String topic) {
        if (handlers == null)
            return null;
        return handlers.remove(topic);
    }

    public void fireEvent(IMessage msg) {
        if (handlers == null)
            return;

        notifyListeners(msg);
    }

    private void notifyListeners(IMessage msg) {
    	IncomingMessageHandler listener = handlers.get(msg.getTopic());
    	if(listener!=null) 	{
    		listener.doEvent(msg);
    	}
    }
}
