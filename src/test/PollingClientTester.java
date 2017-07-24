package test;

import com.xxdb.client.PollingClient;
import com.xxdb.client.TopicPoller;
import com.xxdb.client.datatransferobject.IMessage;
import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicIntVector;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by root on 7/24/17.
 */
public class PollingClientTester {
    public static void main(String args[]) {
        PollingClient client = new PollingClient();

        try {
            TopicPoller poller = client.subscribe("localhost", 8848, "trades", -1);
            int count = 0;
            boolean started = false;
            long start = System.currentTimeMillis();
            while (true) {
                ArrayList<IMessage> msgs = poller.poll(1000);
                if (msgs.size() > 0 && started == false) {
                    started = true;
                    start = System.currentTimeMillis();
                }
                count += msgs.size();
                if (msgs.size() > 0) {
                    if (((BasicInt)msgs.get(msgs.size() - 1).getEntity(2)).getInt() == -1) {
                        break;
                    }
                }
            }
            long end = System.currentTimeMillis();
            System.out.println(count + " messages took " + (end - start) + "ms, throughput: " + count / ((end - start) / 1000.0) + " messages/s");
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.exit(0);
    }
}
