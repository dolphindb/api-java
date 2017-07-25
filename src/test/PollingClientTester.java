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
        PollingClient client = new PollingClient(8992);

        /*
        n=20000000
        t=table(n:0,`time`sym`qty`price`exch`index,[TIMESTAMP,SYMBOL,INT,DOUBLE,SYMBOL,LONG])
        share t as trades
        setStream(trades,true)
        t=NULL
        rows = 1
        timev = take(now(), rows)
        symv = take(`MKFT, rows)
        qtyv = take(112, rows)
        pricev = take(53.75, rows)
        exchv = take(`N, rows)
        for(x in 0:2000000){
        insert into trades values(timev, symv, qtyv, pricev, exchv,x)
        }
        insert into trades values(timev, symv, take(-1, 1), pricev, exchv,x)
         */
        try {
            TopicPoller poller1 = client.subscribe("192.168.1.12", 8082, "trades", -1);
            //TopicPoller poller2 = client.subscribe("192.168.1.25", 8801, "trades2", -1);
            int count = 0;
            boolean started = false;
            long start = System.currentTimeMillis();
            while (true) {
                ArrayList<IMessage> msgs = poller1.poll(1000);
                if (msgs.size() > 0 && started == false) {
                	
                    started = true;
                    start = System.currentTimeMillis();
                }
                
                count += msgs.size();
                //System.out.println("get message " + count);
                if (msgs.size() > 0) {
                    if (((BasicInt)msgs.get(msgs.size() - 1).getEntity(2)).getInt() == -1) {
                        break;
                    }
                }
                long end = System.currentTimeMillis();    
                if((end - start) % 60000 > 0)
                	System.out.println(count + " messages took " + (end - start) + "ms, throughput: " + count / ((end - start) / 1000.0) + " messages/s");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.exit(0);
    }
}
