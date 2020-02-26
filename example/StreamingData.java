import com.xxdb.data.*;
import com.xxdb.DBConnection;
import com.xxdb.data.Vector;
import com.xxdb.streaming.client.*;

import java.io.IOException;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class StreamingData {
    private static DBConnection conn;
    public static String HOST = "localhost";
    public static Integer PORT = 8848;
    public static ThreadedClient client;
    public static char METHOD = 'P';
    public static Integer subscribePORT = 8892;

    public void writeStreamTable() throws IOException {
        conn.login("admin", "123456", false);
        conn.run("share streamTable(30000:0,`id`time`sym`qty`price,[INT,TIME,SYMBOL,INT,DOUBLE]) as Trades\n");
        conn.run("def saveData(data){ Trades.tableInsert(data)}");
    }

    public void PollingClient() throws SocketException {
        PollingClient client = new PollingClient(subscribePORT);
        try {
            TopicPoller poller1 = client.subscribe(HOST, PORT, "Trades");
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
                    System.out.print("time:" + time + " ");
                    String symbol = msgs.get(i).getEntity(2).getString();
                    System.out.print("sym:" + symbol + " ");
                    Integer qty = ((BasicInt) msgs.get(i).getEntity(3)).getInt();
                    System.out.print("qty:" + qty + " ");
                    Double price = ((BasicDouble) msgs.get(i).getEntity(4)).getDouble();
                    System.out.print("price:" + price + " \n");
                }
                if (msgs.size() > 0) {
                    if (((BasicInt) msgs.get(msgs.size() - 1).getEntity(0)).getInt() == -1) {
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
            client.unsubscribe(HOST, PORT, "Trades");
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
            System.out.print("time:" + time + " ");
            String symbol = msg.getEntity(2).getString();
            System.out.print("sym:" + symbol + " ");
            Integer qty = ((BasicInt) msg.getEntity(3)).getInt();
            System.out.print("qty:" + qty + " ");
            Double price = ((BasicDouble) msg.getEntity(4)).getDouble();
            System.out.print("price:" + price + " \n");
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

    public void ThreadedClient() throws SocketException {
        ThreadedClient client = new ThreadedClient(subscribePORT);
        try {
            client.subscribe(HOST, PORT, "Trades", "", new SampleMessageHandler());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length == 4) {
            try {
                HOST = args[0];
                PORT = Integer.parseInt(args[1]);
                subscribePORT = Integer.parseInt(args[2]);
                METHOD = args[3].charAt(0);
                if (METHOD != 'p' && METHOD != 'P' && METHOD != 'T' && METHOD != 't')
                    throw new Exception("the 4th parameter 'subscribeMethod' must be 'P' or 'T'");
            } catch (Exception e) {
                System.out.println("Wrong arguments");
            }
        } else if (args.length != 4 && args.length != 0) {
            System.out.println("wrong arguments");
            return;
        }
        conn = new DBConnection();
        try {
            conn.connect(HOST, PORT);
        } catch (IOException e) {
            System.out.println("Connection error");
            e.printStackTrace();
        }
        try {
            new StreamingData().writeStreamTable();
        } catch (IOException e) {
            System.out.println("Writing error");
        }
        conn.close();
        try {
            switch (METHOD) {
                case 'p':
                case 'P':
                    new StreamingData().PollingClient();
                case 't':
                case 'T':
                    new StreamingData().ThreadedClient();
            }
        } catch (IOException e) {
            System.out.println("Subscription error");
        }
    }
}
