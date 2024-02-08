package com.xxdb.route;

import com.xxdb.data.*;
import junit.framework.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.time.Month.JANUARY;
import static org.junit.Assert.assertEquals;

public class TestDomain {
    private static HashDomain  hashDomain;
    private static ListDomain listDomain;
    private static RangeDomain  rangeDomain ;
    private static ValueDomain   valueDomain  ;


    @Test
    public void test_HashDomain_getPartitionKeys() throws Exception{
        hashDomain = new HashDomain(1, Entity.DATA_TYPE.DT_DATEHOUR, Entity.DATA_CATEGORY.TEMPORAL);
        BasicDateTimeVector b3 = new BasicDateTimeVector(0);
        System.out.println(b3.getDataCategory());
        System.out.println(b3.getDataType());
        List<Integer> result = hashDomain.getPartitionKeys(b3);
        System.out.println(result.toString());
        Assert.assertEquals("[]",result.toString());
    }
    @Test
    public void test_HashDomain_getPartitionKeys_error() throws Exception{
        hashDomain = new HashDomain(1, Entity.DATA_TYPE.DT_INT, Entity.DATA_CATEGORY.ARRAY);
        BasicIntVector b3 = new BasicIntVector(new int[]{1,2,3});
        try {
            List<Integer> result = hashDomain.getPartitionKeys(b3);
        }catch (Exception e){
            assertEquals("Data category incompatible.", e.getMessage());
        }
    }

    @Test
    public void test_HashDomain_getPartitionKey_error() throws Exception{
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
    public void test_HashDomain_getPartitionKey() throws Exception{
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
    public void test_HashDomain_getPartitionKey_1() throws Exception{
        hashDomain = new HashDomain(3, Entity.DATA_TYPE.DT_DATETIME, Entity.DATA_CATEGORY.TEMPORAL);
        Scalar  b3 = new BasicDate(LocalDate.ofEpochDay(1));
        System.out.println(b3.getDataCategory());
        System.out.println(b3.getDataType());
        String re = null;
        try{
            int result = hashDomain.getPartitionKey(b3);
        }catch(RuntimeException ex){
            re = ex.getMessage();
        }
        assertEquals("The target date/time type supports MONTH/DATE only for time being.", re);
        LocalDateTime time = LocalDateTime.of(2022,1,1,1,1,1,10000);
        Scalar  b4 = new BasicDateTime(time);
        int result1 = hashDomain.getPartitionKey(b4);
        assertEquals(1, result1);
    }

    @Test
    public void test_ListDomain_creatListDomainError(){
        try {
            BasicIntVector bi = new BasicIntVector(new int[]{1,2,3});
            listDomain = new ListDomain(bi, Entity.DATA_TYPE.DT_INT, Entity.DATA_CATEGORY.INTEGRAL);
        }catch (RuntimeException e){
            assertEquals("The input list must be a tuple.", e.getMessage());
        }
    }
    @Test
    public void test_ListDomain_getPartitionKeys() throws Exception{
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
    public void test_ListDomain_getPartitionKeys_error() throws Exception{
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
    public void test_ListDomain_getPartitionKeys_partitionCol_null() throws Exception{
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
    public void test_ListDomain_getPartitionKey_error() throws Exception{
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
    public void test_ListDomain_getPartitionKey() throws Exception{
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
    @Test
    public void test_RangeDomain_creatRangeDomain_Error(){
        try {
            BasicIntVector bi = new BasicIntVector(new int[]{1,2,3});
            rangeDomain = new RangeDomain(bi, Entity.DATA_TYPE.DT_INT, Entity.DATA_CATEGORY.INTEGRAL);
        }catch (RuntimeException e){
            assertEquals("The input list must be a tuple.", e.getMessage());
        }
    }
    @Test
    public void test_RangeDomain_getPartitionKeys() throws Exception{
        BasicDateTimeVector ba = new BasicDateTimeVector(new int[]{0,1,2});
        rangeDomain = new RangeDomain(ba, Entity.DATA_TYPE.DT_DATE, Entity.DATA_CATEGORY.TEMPORAL);
        BasicDateTimeVector b3 = new BasicDateTimeVector(new int[]{0,1,2});
        System.out.println(b3.getDataCategory());
        System.out.println(b3.getDataType());
        List<Integer> result = rangeDomain.getPartitionKeys(b3);
        System.out.println(result.toString());
        Assert.assertEquals("[0, 0, 0]",result.toString());
    }
    @Test
    public void test_RangeDomain_getPartitionKeys_error() throws Exception{
        BasicIntVector b1 = new BasicIntVector(new int[]{1,2,3});
        BasicIntVector b2 = new BasicIntVector(new int[]{4,5,6});
        BasicAnyVector ba = new BasicAnyVector(2);
        ba.set(0, b1);
        ba.set(1, b2);
        rangeDomain = new RangeDomain(ba, Entity.DATA_TYPE.DT_INT, Entity.DATA_CATEGORY.ARRAY);
        BasicIntVector b3 = new BasicIntVector(new int[]{1,2,3});
        try {
            List<Integer> result = rangeDomain.getPartitionKeys(b3);
        }catch (Exception e){
            assertEquals("Data category incompatible.", e.getMessage());
        }
    }

    @Test
    public void test_RangeDomain_getPartitionKeys_partitionCol_null() throws Exception{
        BasicIntVector b1 = new BasicIntVector(new int[]{1,2,3});
        BasicIntVector b2 = new BasicIntVector(new int[]{4,5,6});
        BasicAnyVector ba = new BasicAnyVector(2);
        ba.set(0, b1);
        ba.set(1, b2);
        rangeDomain = new RangeDomain(ba, Entity.DATA_TYPE.DT_INT, Entity.DATA_CATEGORY.ARRAY);
        BasicDateVector b3 = new BasicDateVector(new int[]{10, 20, 30});
        try {
            List<Integer> result = rangeDomain.getPartitionKeys(b3);
        }catch (Exception e){
            assertEquals("Data category incompatible.", e.getMessage());
        }
    }
    @Test
    public void test_RangeDomainn_getPartitionKey_error() throws Exception{
        BasicDateTimeVector b1 = new BasicDateTimeVector(new int[]{0,1,2});
        BasicDateTimeVector b2 = new BasicDateTimeVector(new int[]{4,5,6});
        BasicAnyVector ba = new BasicAnyVector(2);
        ba.set(0, b1);
        ba.set(1, b2);
        rangeDomain = new RangeDomain(ba, Entity.DATA_TYPE.DT_DATE, Entity.DATA_CATEGORY.TEMPORAL);
        Scalar  b3 = new BasicInt(1);
        System.out.println(b3.getDataCategory());
        System.out.println(b3.getDataType());
        String re = null;
        try{
            int result = rangeDomain.getPartitionKey(b3);
        }catch(RuntimeException ex){
            re = ex.getMessage();
        }
        assertEquals("Data category incompatible.", re);
    }
    @Test
    public void test_RangeDomain_getPartitionKey() throws Exception{
        BasicDateTimeVector ba = new BasicDateTimeVector(new int[]{0,1,2});
        rangeDomain = new RangeDomain(ba, Entity.DATA_TYPE.DT_DATE, Entity.DATA_CATEGORY.TEMPORAL);
        Scalar  b3 = new BasicDate(LocalDate.ofEpochDay(1));
        System.out.println(b3.getDataCategory());
        System.out.println(b3.getDataType());
        int result = rangeDomain.getPartitionKey(b3);
        assertEquals(1, result);
        LocalDateTime time = LocalDateTime.of(2022,1,1,1,1,1,10000);
        Scalar  b4 = new BasicDateTime(time);
        int result1 = rangeDomain.getPartitionKey(b4);
        assertEquals(-1, result1);
    }
    @Test
    public void test_ValueDomain_creatValueDomain_Error(){
        try {
            BasicIntVector bi = new BasicIntVector(new int[]{1,2,3});
            valueDomain = new ValueDomain(bi, Entity.DATA_TYPE.DT_INT, Entity.DATA_CATEGORY.INTEGRAL);
        }catch (RuntimeException e){
            assertEquals("The input list must be a tuple.", e.getMessage());
        }
    }
    @Test
    public void test_ValueDomain_getPartitionKeys() throws Exception{
        List<String> st = new ArrayList<String>(2);
        st.add("hello");
        st.add("QQQ");
        BasicStringVector ba = new BasicStringVector(st);
        valueDomain = new ValueDomain(ba, Entity.DATA_TYPE.DT_INT, Entity.DATA_CATEGORY.LITERAL);
        Vector tt = new BasicStringVector(new String[] { "hello", "QQQ" });
        List<Integer> result = valueDomain.getPartitionKeys(tt);
        System.out.println(result.toString());
        Assert.assertEquals("[76235, 15193]",result.toString());
    }
    @Test
    public void test_ValueDomain_getPartitionKeys_error() throws Exception{
        BasicIntVector b1 = new BasicIntVector(new int[]{1,2,3});
        BasicIntVector b2 = new BasicIntVector(new int[]{4,5,6});
        BasicAnyVector ba = new BasicAnyVector(2);
        ba.set(0, b1);
        ba.set(1, b2);
        valueDomain = new ValueDomain(ba, Entity.DATA_TYPE.DT_INT, Entity.DATA_CATEGORY.ARRAY);
        BasicIntVector b3 = new BasicIntVector(new int[]{1,2,3});
        String re = null;
        try {
            List<Integer> result = valueDomain.getPartitionKeys(b3);
        }catch (RuntimeException e){
            re = e.getMessage();
        }
        assertEquals("Data category incompatible.", re);
    }
    @Test
    public void test_ValueDomain_getPartitionKeys_error_1() throws Exception{
        BasicLongVector ba = new BasicLongVector(2);
        ba.add((long)1);
        ba.add((long)2);
        valueDomain = new ValueDomain(ba, Entity.DATA_TYPE.DT_LONG, Entity.DATA_CATEGORY.LITERAL);
        Long[] data = {1l,-1l};
        Vector tt = new BasicLongVector(Arrays.asList(data));
        String re = null;
        try {
            List<Integer> result = valueDomain.getPartitionKeys(tt);
        }catch (RuntimeException e){
            re = e.getMessage();
        }
        assertEquals("Long type value can't be used as a partition column.", re);
    }

    @Test
    public void test_ValueDomain_getPartitionKeys_partitionCol_null() throws Exception{
        BasicIntVector b1 = new BasicIntVector(new int[]{1,2,3});
        BasicIntVector b2 = new BasicIntVector(new int[]{4,5,6});
        BasicAnyVector ba = new BasicAnyVector(2);
        ba.set(0, b1);
        ba.set(1, b2);
        valueDomain = new ValueDomain(ba, Entity.DATA_TYPE.DT_INT, Entity.DATA_CATEGORY.ARRAY);
        BasicDateVector b3 = new BasicDateVector(new int[]{10, 20, 30});
        String re = null;
        try {
            List<Integer> result = valueDomain.getPartitionKeys(b3);
        }catch (RuntimeException e){
            re = e.getMessage();
        }
        assertEquals("Data category incompatible.", re);
    }

    @Test
    public void test_ValueDomain_getPartitionKey_error() throws Exception{
        BasicDateTimeVector b1 = new BasicDateTimeVector(new int[]{0,1,2});
        BasicDateTimeVector b2 = new BasicDateTimeVector(new int[]{4,5,6});
        BasicAnyVector ba = new BasicAnyVector(2);
        ba.set(0, b1);
        ba.set(1, b2);
        valueDomain = new ValueDomain(ba, Entity.DATA_TYPE.DT_DATE, Entity.DATA_CATEGORY.TEMPORAL);
        Scalar  b3 = new BasicInt(1);
        System.out.println(b3.getDataCategory());
        System.out.println(b3.getDataType());
        String re = null;
        try{
            int result = valueDomain.getPartitionKey(b3);
        }catch(RuntimeException ex){
            re = ex.getMessage();
        }
        assertEquals("Data category incompatible.", re);
    }
    @Test
    public void test_ValueDomain_getPartitionKey_error_1() throws Exception{
        BasicLongVector ba = new BasicLongVector(2);
        ba.add((long)1);
        ba.add((long)2);
        valueDomain = new ValueDomain(ba, Entity.DATA_TYPE.DT_LONG, Entity.DATA_CATEGORY.LITERAL);
        Scalar tt = new BasicLong((long)2);
        String re = null;
        try {
            int result = valueDomain.getPartitionKey(tt);
        }catch (RuntimeException e){
            re = e.getMessage();
        }
        assertEquals("Long type value can't be used as a partition column.", re);
    }
    @Test
    public void test_ValueDomain_getPartitionKey() throws Exception{
        List<String> st = new ArrayList<String>(2);
        st.add("hello");
        st.add("QQQ");
        BasicStringVector ba = new BasicStringVector(st);
        valueDomain = new ValueDomain(ba, Entity.DATA_TYPE.DT_INT, Entity.DATA_CATEGORY.LITERAL);
        Scalar tt = new BasicString("hello");
        int result = valueDomain.getPartitionKey(tt);
        assertEquals(76235, result);
        Scalar tt1 = new BasicString("QQQ");
        int result1 = valueDomain.getPartitionKey(tt1);
        assertEquals(15193, result1);
    }
}
