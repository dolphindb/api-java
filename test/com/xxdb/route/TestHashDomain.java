package com.xxdb.route;

import com.xxdb.data.*;
import junit.framework.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestHashDomain {
    private static HashDomain  hashDomain;

    @Test
    public void test_getPartitionKeys() throws Exception{
        hashDomain = new HashDomain(1, Entity.DATA_TYPE.DT_DATEHOUR, Entity.DATA_CATEGORY.TEMPORAL);
        BasicDateTimeVector b3 = new BasicDateTimeVector(0);
        System.out.println(b3.getDataCategory());
        System.out.println(b3.getDataType());
        List<Integer> result = hashDomain.getPartitionKeys(b3);
        System.out.println(result.toString());
        Assert.assertEquals("[]",result.toString());
    }
    @Test
    public void test_getPartitionKeys_error() throws Exception{
        hashDomain = new HashDomain(1, Entity.DATA_TYPE.DT_INT, Entity.DATA_CATEGORY.ARRAY);
        BasicIntVector b3 = new BasicIntVector(new int[]{1,2,3});
        try {
            List<Integer> result = hashDomain.getPartitionKeys(b3);
        }catch (Exception e){
            assertEquals("Data category incompatible.", e.getMessage());
        }
    }

    @Test
    public void test_getPartitionKey_error() throws Exception{
        hashDomain = new HashDomain(1, Entity.DATA_TYPE.DT_DATE, Entity.DATA_CATEGORY.TEMPORAL);
        Scalar  b3 = new BasicInt(1);
        System.out.println(b3.getDataCategory());
        System.out.println(b3.getDataType());
        String re = null;
        try{
            int result = hashDomain.getPartitionKey(b3);
        }catch(RuntimeException ex){
            re = ex.getMessage();
        }
        assertEquals("Data category incompatible.", re);
    }
    @Test
    public void test_getPartitionKey() throws Exception{
        hashDomain = new HashDomain(3, Entity.DATA_TYPE.DT_DATE, Entity.DATA_CATEGORY.TEMPORAL);
        Scalar  b3 = new BasicDate(LocalDate.ofEpochDay(1));
        System.out.println(b3.getDataCategory());
        System.out.println(b3.getDataType());
        int result = hashDomain.getPartitionKey(b3);
        assertEquals(1, result);
        LocalDateTime time = LocalDateTime.of(2022,1,1,1,1,1,10000);
        Scalar  b4 = new BasicDateTime(time);
        int result1 = hashDomain.getPartitionKey(b4);
        assertEquals(0, result1);
    }
    @Test
    public void test_getPartitionKey_1() throws Exception{
        hashDomain = new HashDomain(3, Entity.DATA_TYPE.DT_DATETIME, Entity.DATA_CATEGORY.TEMPORAL);
        Scalar  b3 = new BasicDate(LocalDate.ofEpochDay(1));
        System.out.println(b3.getDataCategory());
        System.out.println(b3.getDataType());
        int result = hashDomain.getPartitionKey(b3);
        assertEquals(1, result);
        LocalDateTime time = LocalDateTime.of(2022,1,1,1,1,1,10000);
        Scalar  b4 = new BasicDateTime(time);
        int result1 = hashDomain.getPartitionKey(b4);
        assertEquals(0, result1);
    }
}
