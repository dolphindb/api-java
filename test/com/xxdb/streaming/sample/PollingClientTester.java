/* ==============server publish data sample========== 
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
package com.xxdb.streaming.sample;

import com.xxdb.data.BasicByte;
import com.xxdb.data.BasicDouble;
import com.xxdb.data.BasicInt;
import com.xxdb.route.PartitionedByRangeTableAppenderTest;
import com.xxdb.streaming.client.IMessage;
import com.xxdb.streaming.client.PollingClient;
import com.xxdb.streaming.client.TopicPoller;

import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;

public class PollingClientTester {
    public static void main(String args[]) throws SocketException {
        PollingClient client = new PollingClient(8992);
        
        try {
            TopicPoller poller1 = client.subscribe("192.168.1.25", 8847, "Trades");
            int count = 0;
            boolean started = false;
            long start = System.currentTimeMillis();
            long last = System.currentTimeMillis();
            while (true) {
                ArrayList<IMessage> msgs = poller1.poll(1000);
                if (msgs == null) {
                    count = 0;
                    start = System.currentTimeMillis();
                    continue;
                }
                if (msgs.size() > 0 && started == false) {
                    started = true;
                    start = System.currentTimeMillis();
                }

                count += msgs.size();
                for (int i = 0; i < msgs.size(); ++i) {
                    String symbol = msgs.get(i).getEntity(0).getString();
                    if (!PartitionedByRangeTableAppenderTest.symbolSet.contains(symbol))
                        assert PartitionedByRangeTableAppenderTest.symbolSet.contains(symbol);
                    Double price = ((BasicDouble)msgs.get(i).getEntity(3)).getDouble();
                    if (!PartitionedByRangeTableAppenderTest.doubleSet.contains(price))
                        assert PartitionedByRangeTableAppenderTest.doubleSet.contains(price);
                    Integer size =  ((BasicInt)msgs.get(i).getEntity(4)).getInt();
                    if (!PartitionedByRangeTableAppenderTest.intSet.contains(size))
                        assert PartitionedByRangeTableAppenderTest.intSet.contains(size);
                    byte ex = ((BasicByte)msgs.get(i).getEntity(8)).getByte();
                    if (ex != 'B')
                        assert(ex == 'B');
                }

                //System.out.println("get message " + count);
                if (msgs.size() > 0) {
                    if (((BasicInt)msgs.get(msgs.size() - 1).getEntity(2)).getInt() == -1) {
                        break;
                    }
                }
                long now = System.currentTimeMillis();
                if (now - last >= 1000) {
                    long end = System.currentTimeMillis();
                    System.out.println(count + " messages took " + (end - start) + "ms, throughput: " + count / ((end - start) / 1000.0) + " messages/s");
                    last = now;
                }
            }
            long end = System.currentTimeMillis();
            System.out.println(count + " messages took " + (end - start) + "ms, throughput: " + count / ((end - start) / 1000.0) + " messages/s");
            //client.unsubscribe("192.168.1.45", 8904, "trades1");
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.exit(0);

    }
}
