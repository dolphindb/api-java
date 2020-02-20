import com.xxdb.data.*;
import com.xxdb.DBConnection;
import com.xxdb.data.Vector;
import com.xxdb.streaming.client.*;

import java.io.IOException;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
/**
 * This example shows how to write data to streamTable and subscribe streaming data
 * To subscribe streaming data needs three steps:
 * (1) Java API can acquire streaming data in 2 ways. Choose one way in line 190 and 191.
 * (2) Run this program, and a streamTable will be created and  subscription will be started up.
 * (3) Run following scripts in server to publish data:
 *  for(x in 0:20000){
 *  time =time(now())
 *  sym= rand(`S`M`MS`GO, 1)
 *  qty= rand(1000..2000,1)
 *  price = rand(2335.34,1)
 *  insert into Trades values(x, time,sym, qty, price)
 *  }
 *  insert into Trades values(-1, time,sym, qty, price)
 */
public class StreamingWriteAndSubscribe {
    private static DBConnection conn;
    public static String HOST  = "localhost";
    public static Integer PORT = 8848;
    public static ThreadedClient client;

    private	BasicTable createTable(){
        List<String> colNames = new ArrayList<String>();
        colNames.add("id");
        colNames.add("time");
        colNames.add("sym");
        colNames.add("qty");
        colNames.add("price");
        List<Vector> cols = new ArrayList<Vector>(){};
        //id
        int[] vid = new int[]{0,1};
        BasicIntVector bidv = new BasicIntVector(vid);
        cols.add(bidv);
        //time
        int[] vtime = new int[]{Utils.countMilliseconds(16,46,05,123),Utils.countMilliseconds(18,32,05,321)};
        BasicTimeVector btimev = new BasicTimeVector(vtime);
        cols.add(btimev);
        //sym
        String[] vsymbol = new String[]{"GOOG","MS"};
        BasicStringVector bsymbolv = new BasicStringVector(vsymbol);
        cols.add(bsymbolv);
        //qty
        int[] vint = new int[]{1000,2000};
        BasicIntVector bintv = new BasicIntVector(vint);
        cols.add(bintv);
        //price
        double[] vdouble = new double[]{24546.54,4356.23};
        BasicDoubleVector bdoublev = new BasicDoubleVector(vdouble);
        cols.add(bdoublev);
        BasicTable t1 = new BasicTable(colNames, cols);
        return t1;
    }

    public void write_streamTable() throws IOException {
        BasicTable table1 = createTable();
        conn.login("admin","123456",false);
        conn.run("t = streamTable(30000:0,`id`time`sym`qty`price,[INT,TIME,SYMBOL,INT,DOUBLE])\n");
        conn.run("share t as Trades");
        conn.run("def saveData(data){ Trades.append!(data)}");
        List<Entity> args = new ArrayList<Entity>(1);
        args.add(table1);
        conn.run("saveData", args);
        BasicTable dt = (BasicTable)conn.run("select * from t");
        System.out.println(dt.getString());
        if(dt.rows()!=2){
            System.out.println("failed");
        }
    }

    public void pollingClient()throws SocketException {
        PollingClient client = new PollingClient(8002);
        try {
            TopicPoller poller1 = client.subscribe("localhost", 8848, "Trades");
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
                    BasicTime time = (BasicTime) msgs.get(i).getEntity(1);
                    System.out.print("time:"+time+" ");
                    String symbol = msgs.get(i).getEntity(2).getString();
                    System.out.print("sym:"+symbol+" ");
                    Integer qty =  ((BasicInt)msgs.get(i).getEntity(3)).getInt();
                    System.out.print("qty:"+qty+" ");
                    Double price = ((BasicDouble)msgs.get(i).getEntity(4)).getDouble();
                    System.out.print("price:"+price+" \n");
                }
                if (msgs.size() > 0) {
                    if (((BasicInt)msgs.get(msgs.size() - 1).getEntity(0)).getInt() == -1) {
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
            client.unsubscribe("localhost", 8848, "Trades");
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

    public static class SampleMessageHandler implements MessageHandler {
        private AtomicLong count = new AtomicLong();
        private long start = 0;

        private boolean started = false;
        @Override
        public void doEvent(IMessage msg) {
            if (started == false) {
                started = true;
                start = System.currentTimeMillis();
            }
            BasicTime time = (BasicTime) msg.getEntity(1);
            System.out.print("time:"+time+" ");
            String symbol = msg.getEntity(2).getString();
            System.out.print("sym:"+symbol+" ");
            Integer qty =  ((BasicInt)msg.getEntity(3)).getInt();
            System.out.print("qty:"+qty+" ");
            Double price = ((BasicDouble)msg.getEntity(4)).getDouble();
            System.out.print("price:"+price+" \n");
            count.incrementAndGet();
            System.out.println(count.get());
            if (count.get() % 10000 == 0) {
                long end = System.currentTimeMillis();
                System.out.println(count + " messages took " + (end - start) + "ms, throughput: " + count.get() / ((end - start) / 1000.0) + " messages/s");

            }
            if (count.get() == 20000) {
                System.out.println("Done");
            }
        }
    }

    public void ThreadedClient()throws SocketException
    {
        ThreadedClient client = new ThreadedClient(8997);
        try {
            client.subscribe("localhost", 8848, "Trades", "", new SampleMessageHandler());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        conn = new DBConnection();
        try {
            conn.connect(HOST, PORT);
        } catch (IOException e) {
            System.out.println("Connection error");
            e.printStackTrace();
        }
        try{
            new StreamingWriteAndSubscribe().write_streamTable();
        }catch (IOException e)
        {
            System.out.println("Writing error");
        }
        conn.close();
        try{
            new StreamingWriteAndSubscribe().pollingClient();
            //new StreamingWriteAndSubscribe().ThreadedClient();
        }catch (IOException e)
        {
            System.out.println("Subscription error");
        }
    }
}
