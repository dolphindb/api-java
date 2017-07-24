package com.xxdb.client;
import java.util.EventListener;

import com.xxdb.client.datatransferobject.IMessage;

public interface MessageIncomingHandler extends EventListener {
	public void DoEvent(IMessage msg);
}
