package com.xxdb.streaming.client;
import java.util.EventListener;

import com.xxdb.streaming.client.datatransferobject.IMessage;

public interface MessageHandler extends EventListener {
	void doEvent(IMessage msg);
}
