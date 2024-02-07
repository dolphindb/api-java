package com.xxdb;
import com.xxdb.data.*;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import java.util.ArrayList;
import java.util.List;

public class TestMain {
    public static void main(String[] args) {

        List<Result> results = new ArrayList<>();
        //results.add(doTest(DBConnectionTest.class));
        results.add(doTest(BasicTableTest.class));
        results.add(doTest(UtilsTest.class));
        results.add(doTest(BasicStringTest.class));
        results.add(doTest(BasicLongTest.class));
        results.add(doTest(BasicIntTest.class));
        results.add(doTest(BasicByteTest.class));
        results.add(doTest(BasicShortTest.class));
        results.add(doTest(BasicUuidTest.class));
        results.add(doTest(BasicIPAddrTest.class));
        results.add(doTest(BasicInt128Test.class));

        int failureCount = 0;
        int runCount = 0;
        for (Result result : results) {
            failureCount += result.getFailureCount();
            runCount += result.getRunCount();
        }
        System.out.println(String.format("failed/total cases : %s/%s ",failureCount,runCount));
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