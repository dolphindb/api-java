package com.xxdb.client;

import com.xxdb.client.datatransferobject.IMessage;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;


public class TopicPoller {
    BlockingQueue<IMessage> queue;
    public TopicPoller(BlockingQueue<IMessage> queue) {
        this.queue = queue;
    }
    // Poll as many messages as there are in the topic.
    // If there are no messages for the topic at start,
    // wait at most @timeout milliseconds.
    public ArrayList<IMessage> poll(long timeout){
        ArrayList<IMessage> msgs = new ArrayList<IMessage>();
        IMessage msg = queue.poll();
        if (msg != null) {
            msgs.add(msg);
            while ((msg = queue.poll()) != null) {
                msgs.add(msg);
            }
        } else {
            try {
                msg = queue.poll(timeout, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    msgs.add(msg);
                    while ((msg = queue.poll()) != null) {
                        msgs.add(msg);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return msgs;
    }

    // take one message from the topic, block if necessary
    public IMessage take(){
        try {
            return queue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }
}
