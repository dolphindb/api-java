package com.xxdb.consumer;
import java.util.EventListener;

import com.xxdb.consumer.datatransferobject.IMessage;

public interface MessageIncomingHandler extends EventListener {
	public void DoEvent(IMessage msg);
}
