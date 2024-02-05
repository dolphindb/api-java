package com.xxdb.route;

import com.xxdb.data.*;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.After;
import org.junit.Before;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestListDomain {
    private static ListDomain listDomain;

    @Test
    public void test_creatListDomainError(){
        try {
            BasicIntVector bi = new BasicIntVector(new int[]{1,2,3});
            listDomain = new ListDomain(bi, Entity.DATA_TYPE.DT_INT, Entity.DATA_CATEGORY.INTEGRAL);
        }catch (RuntimeException e){
            assertEquals("The input list must be a tuple.", e.getMessage());
        }
    }
    @Test
    public void test_getPartitionKeys() throws Exception{
        BasicDateTimeVector b1 = new BasicDateTimeVector(new int[]{0,1,2});
        BasicDateTimeVector b2 = new BasicDateTimeVector(new int[]{4,5,6});
        BasicAnyVector ba = new BasicAnyVector(2);
        ba.set(0, b1);
        ba.set(1, b2);
        listDomain = new ListDomain(ba, Entity.DATA_TYPE.DT_DATE, Entity.DATA_CATEGORY.TEMPORAL);
        BasicDateTimeVector b3 = new BasicDateTimeVector(new int[]{0,1,2});
        System.out.println(b3.getDataCategory());
        System.out.println(b3.getDataType());
        List<Integer> result = listDomain.getPartitionKeys(b3);
        System.out.println(result.toString());
        Assert.assertEquals("[-1, -1, -1]",result.toString());
    }
    @Test
    public void test_getPartitionKeys_error() throws Exception{
        BasicIntVector b1 = new BasicIntVector(new int[]{1,2,3});
        BasicIntVector b2 = new BasicIntVector(new int[]{4,5,6});
        BasicAnyVector ba = new BasicAnyVector(2);
        ba.set(0, b1);
        ba.set(1, b2);
        listDomain = new ListDomain(ba, Entity.DATA_TYPE.DT_INT, Entity.DATA_CATEGORY.ARRAY);
        BasicIntVector b3 = new BasicIntVector(new int[]{1,2,3});
        try {
            List<Integer> result = listDomain.getPartitionKeys(b3);
        }catch (Exception e){
            assertEquals("Data category incompatible.", e.getMessage());
        }
    }

    @Test
    public void test_getPartitionKeys_partitionCol_null() throws Exception{
        BasicIntVector b1 = new BasicIntVector(new int[]{1,2,3});
        BasicIntVector b2 = new BasicIntVector(new int[]{4,5,6});
        BasicAnyVector ba = new BasicAnyVector(2);
        ba.set(0, b1);
        ba.set(1, b2);
        listDomain = new ListDomain(ba, Entity.DATA_TYPE.DT_INT, Entity.DATA_CATEGORY.ARRAY);
        BasicDateVector b3 = new BasicDateVector(new int[]{10, 20, 30});
        try {
            List<Integer> result = listDomain.getPartitionKeys(b3);
        }catch (Exception e){
            assertEquals("Data category incompatible.", e.getMessage());
        }
    }
    @Test
    public void test_getPartitionKey_error() throws Exception{
        BasicDateTimeVector b1 = new BasicDateTimeVector(new int[]{0,1,2});
        BasicDateTimeVector b2 = new BasicDateTimeVector(new int[]{4,5,6});
        BasicAnyVector ba = new BasicAnyVector(2);
        ba.set(0, b1);
        ba.set(1, b2);
        listDomain = new ListDomain(ba, Entity.DATA_TYPE.DT_DATE, Entity.DATA_CATEGORY.TEMPORAL);
        Scalar  b3 = new BasicInt(1);
        System.out.println(b3.getDataCategory());
        System.out.println(b3.getDataType());
        String re = null;
        try{
            int result = listDomain.getPartitionKey(b3);
        }catch(RuntimeException ex){
            re = ex.getMessage();
        }
        assertEquals("Data category incompatible.", re);
    }
    @Test
    public void test_getPartitionKey() throws Exception{
        BasicDateTimeVector b1 = new BasicDateTimeVector(new int[]{0,1,2});
        BasicDateTimeVector b2 = new BasicDateTimeVector(new int[]{4,5,6});
        BasicAnyVector ba = new BasicAnyVector(2);
        ba.set(0, b1);
        ba.set(1, b2);
        listDomain = new ListDomain(ba, Entity.DATA_TYPE.DT_DATE, Entity.DATA_CATEGORY.TEMPORAL);
        Scalar  b3 = new BasicDate(LocalDate.ofEpochDay(1));
        System.out.println(b3.getDataCategory());
        System.out.println(b3.getDataType());
        int result = listDomain.getPartitionKey(b3);
        assertEquals(-1, result);
        LocalDateTime time = LocalDateTime.of(2022,1,1,1,1,1,10000);
        Scalar  b4 = new BasicDateTime(time);
        int result1 = listDomain.getPartitionKey(b4);
        assertEquals(-1, result1);
    }
}
