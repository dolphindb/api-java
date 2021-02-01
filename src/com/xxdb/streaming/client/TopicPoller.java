package com.xxdb.streaming.client;

import com.xxdb.streaming.client.IMessage;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class TopicPoller {
    BlockingQueue<List<IMessage>> queue;
    ArrayList<IMessage> cache = null;

    public TopicPoller(BlockingQueue<List<IMessage>> queue) {
        this.queue = queue;
    }

    public void setQueue(BlockingQueue<List<IMessage>> queue) {
        this.queue = queue;
    }

    private void fillCache(long timeout) {
        assert (cache == null);
        List<IMessage> list = null;
        if (cache == null) {
            try {
                if (timeout >= 0)
                    list = queue.poll(timeout, TimeUnit.MILLISECONDS);
                else
                    list = queue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void fillCache(long timeout, int size) {
        assert (cache == null);
        List<IMessage> list = null;
        LocalTime end=LocalTime.now().plusNanos(timeout*1000000);
        while ((list==null||list.size()<size)&&LocalTime.now().isBefore(end)){
            if(list==null)
                list = queue.poll();
            else
                list.addAll(queue.poll());
        }
        if (list != null) {
            cache = new ArrayList<>(list.size());
            cache.addAll(list);
        }
    }

    public ArrayList<IMessage> poll(long timeout) {
        if (cache == null) {
            fillCache(timeout);
        }
        ArrayList<IMessage> cachedMessages = cache;
        cache = null;
        return cachedMessages;
    }

    public ArrayList<IMessage> poll(long timeout, int size) {
        if(size<=0)
            throw new IllegalArgumentException("Size must be greater than zero");
        fillCache(timeout, size);
        ArrayList<IMessage> cachedMessages = cache;
        cache = null;
        return cachedMessages;
    }

    // take one message from the topic, block if necessary
    public IMessage take() {
        if (cache == null)
            fillCache(-1);
        return cache.remove(0);
    }
}
