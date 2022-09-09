package com.xxdb.streaming.client;


import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class TopicPoller {
    BlockingQueue<List<IMessage>> queue;
    List<IMessage> cache=new ArrayList<>();

    public TopicPoller(BlockingQueue<List<IMessage>> queue) {
        this.queue = queue;
    }

    public void setQueue(BlockingQueue<List<IMessage>> queue) {
        this.queue = queue;
    }

    public ArrayList<IMessage> poll(long timeout) {
        return poll(timeout, 1);
    }

    public ArrayList<IMessage> poll(long timeout, int size) {
        if(size <= 0)
            throw new IllegalArgumentException("Size must be greater than zero");
        ArrayList<IMessage> list = new ArrayList<>(cache);
        cache.clear();
        LocalTime end=LocalTime.now().plusNanos(timeout*1000000);
        while (list.size()<size&&LocalTime.now().isBefore(end)){
            try {
                long mileSeconds = ChronoUnit.MILLIS.between(LocalTime.now(), end);
                List<IMessage> tmp = queue.poll(mileSeconds, TimeUnit.MILLISECONDS);
                if(tmp != null){
                    list.addAll(tmp);
                }
            }catch (InterruptedException e){
                return list;
            }
        }
        return list;
    }

    // take one message from the topic, block if necessary
    public IMessage take() {
        while(true) {
            if (cache.isEmpty() == false) {
                IMessage message = cache.get(0);
                cache.remove(0);
                return message;
            }
            try {
                List<IMessage> tmp = queue.take();
                if (tmp != null) {
                    cache.addAll(tmp);
                }
            } catch (InterruptedException e) {
                return null;
            }
        }
    }
}
