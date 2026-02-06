package com.xxdb.replicator;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ReplicatorStatusTest {
    @Test
    public void test_ReplicatorStatus_setHostStatus_label_null() throws IOException {
        ReplicatorStatus status = new ReplicatorStatus();
        status.setHostStatus(null, new ReplicatorStreamStatus());
        Assert.assertEquals(null, status.getHostStatus(null));

        status.setHostStatus(null, null);
        Assert.assertEquals(null, status.getHostStatus(null));

        ReplicatorStreamStatus StreamStatus = new ReplicatorStreamStatus();
        StreamStatus.setDumpedRows(10);
        StreamStatus.setInsertedRows(100);
        status.setHostStatus(null , StreamStatus);
        Assert.assertEquals(null, status.getHostStatus(null));
    }

    @Test
    public void test_ReplicatorStatus_setHostStatus_label_empty() throws IOException {
        ReplicatorStatus status = new ReplicatorStatus();
        status.setHostStatus("", new ReplicatorStreamStatus());
        Assert.assertEquals("ReplicatorStreamStatus{inserted=0, dumped=0, pending=0, errorCode='', errorInfo=''}", status.getHostStatus("").toString());

        status.setHostStatus("", null);
        Assert.assertEquals("ReplicatorStreamStatus{inserted=0, dumped=0, pending=0, errorCode='', errorInfo=''}", status.getHostStatus("").toString());

        ReplicatorStreamStatus StreamStatus = new ReplicatorStreamStatus();
        StreamStatus.setDumpedRows(10);
        StreamStatus.setInsertedRows(100);
        status.setHostStatus("" , StreamStatus);
        Assert.assertEquals("ReplicatorStreamStatus{inserted=100, dumped=10, pending=90, errorCode='', errorInfo=''}", status.getHostStatus("").toString());
    }

    @Test
    public void test_ReplicatorStatus_setHostStatus() throws IOException {
        ReplicatorStatus status = new ReplicatorStatus();
        ReplicatorStreamStatus StreamStatus = new ReplicatorStreamStatus();
        StreamStatus.setDumpedRows(10);
        StreamStatus.setInsertedRows(100);
        status.setHostStatus("中文11qq@@@@" , StreamStatus);
        Assert.assertEquals("{中文11qq@@@@=ReplicatorStreamStatus{inserted=100, dumped=10, pending=90, errorCode='', errorInfo=''}}", status.getHostStatuses().toString());
        Assert.assertEquals("ReplicatorStreamStatus{inserted=100, dumped=10, pending=90, errorCode='', errorInfo=''}", status.getHostStatus("中文11qq@@@@").toString());

        Assert.assertEquals(90,status.getTotalPendingRows());
        Assert.assertEquals(10,status.getTotalDumpedRows());
        Assert.assertEquals(100, status.getTotalRows());
    }

    @Test
    public void test_ReplicatorStatus_setHostStatuses_null() throws IOException {
        ReplicatorStatus status = new ReplicatorStatus();
        Map<String, ReplicatorStreamStatus> hostStatuses = new HashMap<>();
        status.setHostStatuses(hostStatuses);
        System.out.println(status.getHostStatuses());
        Assert.assertEquals("{}", status.getHostStatuses().toString());
        Assert.assertEquals(null, status.getHostStatus("label2"));
        Assert.assertEquals(0,status.getTotalPendingRows());
        Assert.assertEquals(0,status.getTotalDumpedRows());
        Assert.assertEquals(0, status.getTotalRows());

        status.setHostStatuses(null);
        Assert.assertEquals("{}", status.getHostStatuses().toString());
    }

    @Test
    public void test_ReplicatorStatus_setHostStatuses() throws IOException {
        ReplicatorStatus status = new ReplicatorStatus();
        Map<String, ReplicatorStreamStatus> hostStatuses = new HashMap<>();
        ReplicatorStreamStatus StreamStatus1 = new ReplicatorStreamStatus();
        StreamStatus1.setDumpedRows(10);
        StreamStatus1.setInsertedRows(1000);
        ReplicatorStreamStatus StreamStatus2 = new ReplicatorStreamStatus();
        StreamStatus2.setDumpedRows(10);
        StreamStatus2.setInsertedRows(100);

        hostStatuses.put("label1", StreamStatus1);
        hostStatuses.put("label2",StreamStatus2);
        status.setHostStatuses(hostStatuses);
        System.out.println(status.getHostStatuses());
        Assert.assertEquals("{label1=ReplicatorStreamStatus{inserted=1000, dumped=10, pending=990, errorCode='', errorInfo=''}, label2=ReplicatorStreamStatus{inserted=100, dumped=10, pending=90, errorCode='', errorInfo=''}}", status.getHostStatuses().toString());
        Assert.assertEquals("ReplicatorStreamStatus{inserted=100, dumped=10, pending=90, errorCode='', errorInfo=''}", status.getHostStatus("label2").toString());
        Assert.assertEquals(1080,status.getTotalPendingRows());
        Assert.assertEquals(20,status.getTotalDumpedRows());
        Assert.assertEquals(1100, status.getTotalRows());
    }

    @Test
    public void test_ReplicatorStatus_hasAnyError() throws IOException {
        ReplicatorStatus status = new ReplicatorStatus();
        Assert.assertEquals(false,status.hasAnyError());

        ReplicatorStreamStatus StreamStatus = new ReplicatorStreamStatus();
        StreamStatus.setDumpedRows(10);
        StreamStatus.setInsertedRows(100);
        status.setHostStatus("中文11qq@@@@" , StreamStatus);
        Assert.assertEquals(false,status.hasAnyError());

        ReplicatorStreamStatus StreamStatus1 = new ReplicatorStreamStatus();
        StreamStatus1.setDumpedRows(10);
        StreamStatus1.setInsertedRows(100);
        StreamStatus1.setErrorCode("A1");
        StreamStatus1.setErrorInfo("test");
        status.setHostStatus("中文11qq@@@@" , StreamStatus1);
        Assert.assertEquals(true,status.hasAnyError());
    }

    @Test
    public void test_ReplicatorStatus_toString() throws IOException {
        ReplicatorStatus status = new ReplicatorStatus();
        System.out.println(status.toString());
        Assert.assertEquals("ReplicatorStatus{totalRows=0, totalDumped=0, totalPending=0\r\n" +
                "  Host Statuses: (none)}", status.toString());
        ReplicatorStreamStatus StreamStatus = new ReplicatorStreamStatus();
        status.setHostStatus("中文11qq@@@@" , StreamStatus);
        System.out.println(status.toString());
        Assert.assertEquals("ReplicatorStatus{totalRows=0, totalDumped=0, totalPending=0\r\n" +
                "  Host Statuses:\r\n" +
                "    中文11qq@@@@: ReplicatorStreamStatus{inserted=0, dumped=0, pending=0, errorCode='', errorInfo=''}}", status.toString());
        Map<String, ReplicatorStreamStatus> hostStatuses = new HashMap<>();
        status.setHostStatuses(hostStatuses);

        ReplicatorStreamStatus StreamStatus1 = new ReplicatorStreamStatus();
        StreamStatus1.setDumpedRows(10);
        StreamStatus1.setInsertedRows(1000);
        ReplicatorStreamStatus StreamStatus2 = new ReplicatorStreamStatus();
        StreamStatus2.setDumpedRows(10);
        StreamStatus2.setInsertedRows(100);

        hostStatuses.put("label1", StreamStatus1);
        hostStatuses.put("label2",StreamStatus2);
        status.setHostStatuses(hostStatuses);
        System.out.println(status.toString());
        Assert.assertEquals("ReplicatorStatus{totalRows=1100, totalDumped=20, totalPending=1080\r\n" +
                "  Host Statuses:\r\n" +
                "    label1: ReplicatorStreamStatus{inserted=1000, dumped=10, pending=990, errorCode='', errorInfo=''}\r\n" +
                "    label2: ReplicatorStreamStatus{inserted=100, dumped=10, pending=90, errorCode='', errorInfo=''}}", status.toString());
    }
}
