package com.xxdb;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

public class TestMain {
    public static void main(String[] args) {
        Result result = JUnitCore.runClasses(DBConnectionTest.class);
        for (Failure failure : result.getFailures()) {
            System.out.println(failure.toString());
        }
        System.out.println(String.format("failed/total cases : %s/%s ",result.getFailureCount(), result.getRunCount()));
    }
}