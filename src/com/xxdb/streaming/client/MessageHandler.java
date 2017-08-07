package com.xxdb.streaming.client;
import java.util.EventListener;

public interface MessageHandler extends EventListener {
	void doEvent(IMessage msg);
}
