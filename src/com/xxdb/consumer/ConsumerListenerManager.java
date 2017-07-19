package com.xxdb.consumer;
import java.util.HashSet;
import java.util.Iterator;

import com.xxdb.consumer.datatransferobject.IMessage;

public class ConsumerListenerManager {

	private HashSet listeners;
	/**
     * 添加事件
     * 
     * @param listener
     *            MessageIncomingListener
     */
    public void addMessageIncomingListener(MessageIncomingHandler listener) {
        if (listeners == null) {
            listeners = new HashSet();
        }
        listeners.add(listener);
    }

    /**
     * 移除事件
     * 
     * @param listener
     *            MessageIncomingListener
     */
    public void removeMessageIncomingListener(MessageIncomingHandler listener) {
        if (listeners == null)
            return;
        listeners.remove(listener);
    }

    /**
     * 触发事件
     */
    public void fireEvent(IMessage msg) {
        if (listeners == null)
            return;
        //MessageIncomingEvent event = new MessageIncomingEvent(this, obj);
        notifyListeners(msg);
    }

    /**
     * 通知所有的MessageIncomingListener
     */
    private void notifyListeners(IMessage msg) {
        Iterator iter = listeners.iterator();
        while (iter.hasNext()) {
            MessageIncomingHandler listener = (MessageIncomingHandler) iter.next();
            listener.DoEvent(msg);
        }
    }
}
