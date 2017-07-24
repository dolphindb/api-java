package com.xxdb.client;
import java.util.HashMap;
import com.xxdb.client.datatransferobject.IMessage;

public class ConsumerListenerManager {


	private HashMap<String, MessageIncomingHandler> listeners ;

    public void addMessageIncomingListener(String topic,MessageIncomingHandler listener) {
        if (listeners == null) {
            listeners = new HashMap<String,MessageIncomingHandler>();
        }
        listeners.put(topic,listener);
    }


    public MessageIncomingHandler removeMessageIncomingListener(String topic) {
        if (listeners == null)
            return null;
        return listeners.remove(topic);
    }


    public void fireEvent(IMessage msg) {
        if (listeners == null)
            return;

        notifyListeners(msg);
    }


    private void notifyListeners(IMessage msg) {

    	MessageIncomingHandler listener = listeners.get(msg.getTopic());
    	if(listener!=null) 	{
    		listener.DoEvent(msg);
    	}
    	
    }
}
