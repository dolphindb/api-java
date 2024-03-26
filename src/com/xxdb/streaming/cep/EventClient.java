package com.xxdb.streaming.cep;

import com.xxdb.DBConnection;
import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.data.Entity;
import com.xxdb.data.Utils;
import com.xxdb.streaming.client.AbstractClient;
import com.xxdb.streaming.client.IMessage;
import com.xxdb.streaming.client.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class EventClient extends AbstractClient {

    private EventHandler eventHandler;

    private static final Logger log = LoggerFactory.getLogger(DBConnection.class);

    public EventClient(List<EventScheme> eventSchemes, List<String> eventTimeKeys, List<String> commonKeys) throws SocketException {
        super(0);
        eventHandler = new EventHandler(eventSchemes, eventTimeKeys, commonKeys);
    }

    public void subscribe(String host, int port, String tableName, String actionName, MessageHandler handler, long offset, boolean reconnect, String userName, String password) throws IOException {
        if (Utils.isEmpty(tableName))
            throw new IllegalArgumentException("EventClient subscribe 'tableName' param cannot be null or empty.");

        if (Utils.isEmpty(actionName))
            actionName = DEFAULT_ACTION_NAME;

        BlockingQueue<List<IMessage>> queue = subscribeInternal(host, port, tableName, actionName, handler, offset, reconnect, null, null, false, userName, password, false);
        if (queue == null) {
            System.err.println("Subscription already made, handler loop not created.");
            return;
        }

        Thread thread = new Thread(() -> {
            log.info("EventClient subscribe start.");
            IMessage msg;
            List<String> eventTypes = new ArrayList<>();
            List<List<Entity>> attributes = new ArrayList<>();
            ErrorCodeInfo errorInfo = new ErrorCodeInfo();
            boolean foundNull = false;
            while (!foundNull) {
                List<IMessage> msgs;
                try {
                    msgs = queue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                if (msgs.size() == 0) {
                    foundNull = true;
                    break;
                }
                eventTypes.clear();
                attributes.clear();

                // todo notice，here MessageParser handled col to row;
                if (!eventHandler.deserializeEvent(msgs, eventTypes, attributes, errorInfo)) {
                    System.out.println("deserialize fail " + errorInfo.getErrorInfo());
                    continue;
                }

                int rowSize = eventTypes.size();
                for (int i = 0; i < rowSize; ++i)
                    ((EventMessageHandler) handler).doEvent(eventTypes.get(i), attributes.get(i));

            }
            log.info("nht handle exit.");
        });
        thread.start();
    }

    public void unsubscribe(String host, int port, String tableName, String actionName) throws IOException {
        if (Utils.isEmpty(actionName))
            actionName = DEFAULT_ACTION_NAME;
        unsubscribeInternal(host, port, tableName, actionName);
    }


    @Override
    protected boolean doReconnect(Site site) {
        return false;
    }
}
