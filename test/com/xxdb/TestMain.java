package com.xxdb;
import com.xxdb.data.*;
import com.xxdb.streaming.client.MessageHandler;
import com.xxdb.streaming.client.ThreadedClient;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static java.lang.Thread.sleep;

public class TestMain {
//    public static void main(String[] args) {
//
//        List<Result> results = new ArrayList<>();
//        //results.add(doTest(DBConnectionTest.class));
//        results.add(doTest(BasicTableTest.class));
//        results.add(doTest(UtilsTest.class));
//        results.add(doTest(BasicStringTest.class));
//        results.add(doTest(BasicLongTest.class));
//        results.add(doTest(BasicIntTest.class));
//        results.add(doTest(BasicByteTest.class));
//        results.add(doTest(BasicShortTest.class));
//        results.add(doTest(BasicUuidTest.class));
//        results.add(doTest(BasicIPAddrTest.class));
//        results.add(doTest(BasicInt128Test.class));
//
//        int failureCount = 0;
//        int runCount = 0;
//        for (Result result : results) {
//            failureCount += result.getFailureCount();
//            runCount += result.getRunCount();
//        }
//        System.out.println(String.format("failed/total cases : %s/%s ",failureCount,runCount));
//    }


    public static void main(String[] args) throws IOException, InterruptedException {

        DBConnection conn = new DBConnection();
        conn.connect("192.168.100.3", 38848, "admin", "123456");
        conn.run(
                "share streamTable(select * from loadTable(\"dfs://level2_data\",\"entrust\") limit 1000) as ttt;");// 10000000
        final int[] totalMessageNum = { 0 };
        CountDownLatch latch = new CountDownLatch(1);
        MessageHandler handler = msg -> {
            totalMessageNum[0]++;
            if (totalMessageNum[0] == 1000) {
                latch.countDown();
            }
        };
        ThreadedClient client = new ThreadedClient(0);
        client.subscribe("192.168.100.3", 38848, "ttt", "javaStreamingAPI", handler, 0, false, null,
                null, false);
        latch.await();
        client.unsubscribe("192.168.100.3", 38848, "ttt", "javaStreamingAPI");
        conn.run("undef(`ttt, SHARED);go");
        conn.close();
//        sleep(1000);
        System.out.println("123");
        client.close();
//        System.exit(0);
    }

    private static Result doTest(Class<?> cls){
        System.out.println("Running " + cls.getName() );
        Result result = JUnitCore.runClasses(cls);
        for (Failure failure : result.getFailures()) {
            System.out.println(failure.toString());
        }
        return result;
        //System.out.println(String.format("failed/total cases : %s/%s ",result.getFailureCount(), result.getRunCount()));
    }
}