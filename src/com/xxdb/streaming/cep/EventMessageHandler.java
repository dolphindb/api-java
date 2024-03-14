package com.xxdb.streaming.cep;

import com.xxdb.data.Entity;
import com.xxdb.streaming.client.IMessage;
import com.xxdb.streaming.client.MessageHandler;
import java.util.List;

public class EventMessageHandler implements MessageHandler {

    @Override
    public void doEvent(IMessage msg) {

    }

    public void doEvent(String eventType, List<Entity> attribute) {

    }

}
