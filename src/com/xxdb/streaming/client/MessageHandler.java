package com.xxdb.streaming.client;

import java.util.EventListener;
import java.util.List;

public interface MessageHandler extends EventListener {
    void doEvent(IMessage msg);
    void batchHandler(List<IMessage> msgs);
}
