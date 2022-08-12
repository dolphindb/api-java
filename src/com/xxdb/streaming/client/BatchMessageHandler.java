package com.xxdb.streaming.client;

import java.util.List;

public interface BatchMessageHandler extends MessageHandler {
    void batchHandler(List<IMessage> msgs);
}
