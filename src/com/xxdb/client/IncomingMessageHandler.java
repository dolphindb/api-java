package com.xxdb.client;
import java.util.EventListener;

import com.xxdb.client.datatransferobject.IMessage;

public interface IncomingMessageHandler extends EventListener {
	public void doEvent(IMessage msg);
}
