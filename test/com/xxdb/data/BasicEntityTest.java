package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.io.*;
import org.junit.Assert;
import org.junit.Test;

import javax.management.RuntimeErrorException;

import static org.junit.Assert.*;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.Date;

public class BasicEntityTest {
    @Test
    public void test_typeToCategory(){
        assertEquals(Entity.DATA_CATEGORY.LOGICAL,Entity.typeToCategory(Entity.DATA_TYPE.DT_BOOL));
        assertEquals(Entity.DATA_CATEGORY.BINARY,Entity.typeToCategory(Entity.DATA_TYPE.DT_INT128));
        assertEquals(Entity.DATA_CATEGORY.BINARY,Entity.typeToCategory(Entity.DATA_TYPE.DT_UUID));
        assertEquals(Entity.DATA_CATEGORY.BINARY,Entity.typeToCategory(Entity.DATA_TYPE.DT_IPADDR));
        assertEquals(Entity.DATA_CATEGORY.MIXED,Entity.typeToCategory(Entity.DATA_TYPE.DT_ANY));
        assertEquals(Entity.DATA_CATEGORY.NOTHING,Entity.typeToCategory(Entity.DATA_TYPE.DT_VOID));
        assertEquals(Entity.DATA_CATEGORY.SYSTEM,Entity.typeToCategory(Entity.DATA_TYPE.DT_DURATION));
        assertEquals(Entity.DATA_CATEGORY.FLOATING,Entity.typeToCategory(Entity.DATA_TYPE.DT_FLOAT));
        assertEquals(Entity.DATA_CATEGORY.FLOATING,Entity.typeToCategory(Entity.DATA_TYPE.DT_DOUBLE));
    }

    @Test(expected = RuntimeException.class)
    public void test_Entity_valueOf(){
        Entity.DATA_TYPE.valueOf(100);
    }

    @Test(expected = RuntimeException.class)
    public void test_Entity_valueOfTypeName(){
        Entity.DATA_TYPE.valueOfTypeName("POINT");
        Entity.DATA_TYPE.valueOfTypeName("VECTOR");
    }

    @Test(expected = IOException.class)
    public void test_writeCompressed() throws IOException {
        Entity entity = new BasicInt(4);
        entity.writeCompressed(new BigEndianDataOutputStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                System.out.println(b);
            }
        }));
    }
//    @Test
//    public void test_Entity_isNull() throws IOException {
//        Entity entity = (Entity)new BasicInt(1);
//        System.out.println("basicInt: " + entity.isNull());
//        assertEquals(false,entity.isNull());
//
//    }
    @Test
    public void test_Entity_setNull() throws IOException {
        Scalar entity = new BasicInt(1);
        entity.setNull();
        System.out.println("basicInt: " + entity.isNull());
        assertEquals(true,entity.isNull());
    }
    @Test
    public void test_Entity_getNumber() throws Exception {
        Scalar entity = new BasicInt(1);
        System.out.println("basicInt: " + entity.getNumber());
        assertEquals(1,entity.getNumber());

    }

    @Test
    public void test_Entity_getTemporal() throws Exception {
        Scalar entity = new BasicDate(1);
        System.out.println("basicInt: " + entity.getTemporal());
        assertEquals("1970-01-02",entity.getTemporal().toString());
    }
    @Test
    public void test_Entity_hashBucket() throws Exception {
        Scalar entity = new BasicDate(1);
        System.out.println("basicInt: " + entity.hashBucket(1));
    }
    @Test
    public void test_Entity_toJSONString() throws Exception {
        Scalar entity = new BasicInt(1);
        String re = JSONObject.toJSONString(entity);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"INTEGRAL\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_INT\",\"dictionary\":false,\"int\":1,\"jsonString\":\"1\",\"matrix\":false,\"null\":false,\"number\":1,\"pair\":false,\"scalar\":true,\"scale\":0,\"string\":\"1\",\"table\":false,\"vector\":false}", re);
    }
    @Test
    public void test_BasicEntityFactory_createEntity() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        ExtendedDataInput in = new ExtendedDataInput() {
            @Override
            public boolean isLittleEndian() {
                return false;
            }
            @Override
            public String readString() throws IOException {
                return "Dolphindb";
            }

            @Override
            public Long2 readLong2() throws IOException {
                return null;
            }

            @Override
            public Double2 readDouble2() throws IOException {
                return null;
            }

            @Override
            public byte[] readBlob() throws IOException {
                return new byte[0];
            }

            @Override
            public void readFully(byte[] b) throws IOException {

            }

            @Override
            public void readFully(byte[] b, int off, int len) throws IOException {

            }

            @Override
            public int skipBytes(int n) throws IOException {
                return 0;
            }

            @Override
            public boolean readBoolean() throws IOException {
                return false;
            }

            @Override
            public byte readByte() throws IOException {
                return 0;
            }

            @Override
            public int readUnsignedByte() throws IOException {
                return 0;
            }

            @Override
            public short readShort() throws IOException {
                return 0;
            }

            @Override
            public int readUnsignedShort() throws IOException {
                return 0;
            }

            @Override
            public char readChar() throws IOException {
                return 0;
            }

            @Override
            public int readInt() throws IOException {
                return 1;
            }

            @Override
            public long readLong() throws IOException {
                return 0;
            }

            @Override
            public float readFloat() throws IOException {
                return 0;
            }

            @Override
            public double readDouble() throws IOException {
                return 0;
            }

            @Override
            public String readLine() throws IOException {
                return null;
            }

            @Override
            public String readUTF() throws IOException {
                return null;
            }
        };
        BasicChunkMeta  re = (BasicChunkMeta)factory.createEntity(Entity.DATA_FORM.DF_CHUNK, Entity.DATA_TYPE.DT_INT, in, false);
        assertEquals("DF_CHUNK",re.getDataForm().toString());
        BasicAnyVector  re1 = (BasicAnyVector)factory.createEntity(Entity.DATA_FORM.DF_PAIR, Entity.DATA_TYPE.DT_ANY, in, false);
        assertEquals("DF_VECTOR",re1.getDataForm().toString());
        BasicAnyVector  re2 = (BasicAnyVector)factory.createEntity(Entity.DATA_FORM.DF_VECTOR, Entity.DATA_TYPE.DT_ANY, in, false);
        assertEquals("DF_VECTOR",re2.getDataForm().toString());
        String re3 = null;
        try{
            factory.createEntity(Entity.DATA_FORM.DF_SYSOBJ, Entity.DATA_TYPE.DT_ANY, in, false);
        }catch(Exception ex){
            re3 = ex.getMessage();
        }
        assertEquals("Data type DT_ANY is not supported yet.",re3);
        String re4 = null;
        try{
            factory.createEntity(Entity.DATA_FORM.DF_SYSOBJ, Entity.DATA_TYPE.DT_SYMBOL, in, false);
        }catch(Exception ex){
            re4 = ex.getMessage();
        }
        assertEquals("Data form DF_SYSOBJ is not supported yet.",re4);
    }

    @Test
    public void test_BasicEntityFactory_createMatrixWithDefaultValue() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        ExtendedDataInput in = new ExtendedDataInput() {
            @Override
            public boolean isLittleEndian() {
                return false;
            }

            @Override
            public String readString() throws IOException {
                return "Dolphindb";
            }

            @Override
            public Long2 readLong2() throws IOException {
                return null;
            }

            @Override
            public Double2 readDouble2() throws IOException {
                return null;
            }

            @Override
            public byte[] readBlob() throws IOException {
                return new byte[0];
            }

            @Override
            public void readFully(byte[] b) throws IOException {

            }

            @Override
            public void readFully(byte[] b, int off, int len) throws IOException {

            }

            @Override
            public int skipBytes(int n) throws IOException {
                return 0;
            }

            @Override
            public boolean readBoolean() throws IOException {
                return false;
            }

            @Override
            public byte readByte() throws IOException {
                return 0;
            }

            @Override
            public int readUnsignedByte() throws IOException {
                return 0;
            }

            @Override
            public short readShort() throws IOException {
                return 0;
            }

            @Override
            public int readUnsignedShort() throws IOException {
                return 0;
            }

            @Override
            public char readChar() throws IOException {
                return 0;
            }

            @Override
            public int readInt() throws IOException {
                return 1;
            }

            @Override
            public long readLong() throws IOException {
                return 0;
            }

            @Override
            public float readFloat() throws IOException {
                return 0;
            }

            @Override
            public double readDouble() throws IOException {
                return 0;
            }

            @Override
            public String readLine() throws IOException {
                return null;
            }

            @Override
            public String readUTF() throws IOException {
                return null;
            }
        };
        Matrix  re = (Matrix)factory.createMatrixWithDefaultValue(Entity.DATA_TYPE.DT_INT,2,2);
        assertEquals("#0 #1\n" +
                "0  0 \n" +
                "0  0 \n",re.getString());
        factory.createMatrixWithDefaultValue(Entity.DATA_TYPE.DT_INT128_ARRAY,2,2);
    }
    @Test
    public void test_BasicEntityFactory_createPairWithDefaultValue() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        ExtendedDataInput in = new ExtendedDataInput() {
            @Override
            public boolean isLittleEndian() {
                return false;
            }

            @Override
            public String readString() throws IOException {
                return "Dolphindb";
            }

            @Override
            public Long2 readLong2() throws IOException {
                return null;
            }

            @Override
            public Double2 readDouble2() throws IOException {
                return null;
            }

            @Override
            public byte[] readBlob() throws IOException {
                return new byte[0];
            }

            @Override
            public void readFully(byte[] b) throws IOException {

            }

            @Override
            public void readFully(byte[] b, int off, int len) throws IOException {

            }

            @Override
            public int skipBytes(int n) throws IOException {
                return 0;
            }

            @Override
            public boolean readBoolean() throws IOException {
                return false;
            }

            @Override
            public byte readByte() throws IOException {
                return 0;
            }

            @Override
            public int readUnsignedByte() throws IOException {
                return 0;
            }

            @Override
            public short readShort() throws IOException {
                return 0;
            }

            @Override
            public int readUnsignedShort() throws IOException {
                return 0;
            }

            @Override
            public char readChar() throws IOException {
                return 0;
            }

            @Override
            public int readInt() throws IOException {
                return 1;
            }

            @Override
            public long readLong() throws IOException {
                return 0;
            }

            @Override
            public float readFloat() throws IOException {
                return 0;
            }

            @Override
            public double readDouble() throws IOException {
                return 0;
            }

            @Override
            public String readLine() throws IOException {
                return null;
            }

            @Override
            public String readUTF() throws IOException {
                return null;
            }
        };
        Vector  re = (Vector)factory.createPairWithDefaultValue(Entity.DATA_TYPE.DT_INT);
        assertEquals("[0,0]",re.getString());
        //factory.createPairWithDefaultValue(Entity.DATA_TYPE.DT_INT128_ARRAY,2,2);
    }
    @Test
    public void test_BasicEntityFactory_createScalar() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        BasicByteVector  re = (BasicByteVector)factory.createScalar(Entity.DATA_TYPE.DT_BYTE_ARRAY, new Character[]{1},1);
        assertEquals("[1]",re.getString());

        BasicByteVector  re1 = (BasicByteVector)factory.createScalar(Entity.DATA_TYPE.DT_BYTE_ARRAY, new char[]{1},1);
        assertEquals("[1]",re1.getString());

        BasicDateVector  re2 = (BasicDateVector)factory.createScalar(Entity.DATA_TYPE.DT_DATE_ARRAY, new Date[]{new Date(1)},1);
        assertEquals("1970.01.01",re2.getString(0));

        BasicDateVector  re3 = (BasicDateVector)factory.createScalar(Entity.DATA_TYPE.DT_DATE_ARRAY, new Date[]{new Date(0)},1);
        assertEquals("1970.01.01",re3.getString(0));

        BasicDateVector  re4 = (BasicDateVector)factory.createScalar(Entity.DATA_TYPE.DT_DATE_ARRAY, new Date[]{new Date(-2)},1);
        assertEquals("1970.01.01",re4.getString(0));

        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, 2022);
        calendar.set(Calendar.MONTH, Calendar.JANUARY);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 10);
        calendar.set(Calendar.MINUTE, 30);
        calendar.set(Calendar.SECOND, 0);
        BasicDateTime  re5 = (BasicDateTime)factory.createScalar(Entity.DATA_TYPE.DT_DATETIME, calendar,1);
        assertEquals("2022.01.01T10:30:00",re5.getString());

        Calendar[] calendars = new Calendar[3];
        for (int i = 0; i < calendars.length; i++) {
            calendars[i] = Calendar.getInstance();
        }
        calendars[0].set(Calendar.YEAR, 2022);
        calendars[0].set(Calendar.MONTH, Calendar.JANUARY);
        calendars[0].set(Calendar.DAY_OF_MONTH, 1);

        calendars[1].set(Calendar.YEAR, 2022);
        calendars[1].set(Calendar.MONTH, Calendar.FEBRUARY);
        calendars[1].set(Calendar.DAY_OF_MONTH, 14);

        calendars[2].set(Calendar.YEAR, 2022);
        calendars[2].set(Calendar.MONTH, Calendar.MARCH);
        calendars[2].set(Calendar.DAY_OF_MONTH, 25);
        BasicDateVector  re6 = (BasicDateVector)factory.createScalar(Entity.DATA_TYPE.DT_DATE_ARRAY, calendars,1);
        assertEquals("[2022.01.01,2022.02.14,2022.03.25]",re6.getString());

        Entity[] entitys = new Entity[]{new BasicInt(4),new BasicInt(0),new BasicInt(-4)};
        BasicIntVector  re7 = (BasicIntVector)factory.createScalar(Entity.DATA_TYPE.DT_INT_ARRAY, entitys,1);
        assertEquals("[4,0,-4]",re7.getString());
        String ex = null;
        try{
            BasicLong  re8 = (BasicLong)factory.createScalar(Entity.DATA_TYPE.DT_LONG, new Long2(1,1),1);
        }catch(Exception e){
            ex = e.getMessage();
        }
        assertEquals("Failed to insert data. invalid data type for DT_LONG.",ex);
    }
    @Test
    public void test_BasicEntityFactory_createScalar_createAnyVector_T() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        Float[]  Float1 = new Float[]{-1f,0f,2f};
        String ex = null;
        try{
            BasicDoubleVector  re = (BasicDoubleVector)factory.createScalar(Entity.DATA_TYPE.DT_DOUBLE, Float1,0);
        }catch(Exception e){
            ex = e.getMessage();
        }
        assertEquals("Failed to insert data, only arrayVector support data vector for DT_DOUBLE.",ex);
    }
    @Test
    public void test_BasicEntityFactory_createScalar_createAnyVector_float() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        float[]  float1 = new float[]{-1,0,2};
        BasicDoubleVector  re = (BasicDoubleVector)factory.createScalar(Entity.DATA_TYPE.DT_DOUBLE_ARRAY, float1,0);
        assertEquals("[-1,0,2]",re.getString());
        String ex = null;
        try{
            BasicDoubleVector  re1 = (BasicDoubleVector)factory.createScalar(Entity.DATA_TYPE.DT_DOUBLE, float1,0);
        }catch(Exception e){
            ex = e.getMessage();
        }
        assertEquals("Failed to insert data, only arrayVector support data vector for DT_DOUBLE.",ex);
    }
    @Test
    public void test_BasicEntityFactory_createScalar_createAnyVector_double() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        double[]  double1 = new double[]{-1,0,2};
        BasicFloatVector  re = (BasicFloatVector)factory.createScalar(Entity.DATA_TYPE.DT_FLOAT_ARRAY, double1,0);
        assertEquals("[-1,0,2]",re.getString());
    }
    @Test
    public void test_BasicEntityFactory_createScalar_createAnyVector_int() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        int[]  int1 = new int[]{1,2,2};
        BasicLongVector  re = (BasicLongVector)factory.createScalar(Entity.DATA_TYPE.DT_LONG_ARRAY, int1,0);
        assertEquals("[1,2,2]",re.getString());
    }
    @Test
    public void test_BasicEntityFactory_createScalar_createAnyVector_short() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        short[]  short1 = new short[]{1,2,2};
        BasicIntVector  re = (BasicIntVector)factory.createScalar(Entity.DATA_TYPE.DT_INT_ARRAY, short1,0);
        assertEquals("[1,2,2]",re.getString());
    }
    @Test
    public void test_BasicEntityFactory_createScalar_createAnyVector_byte() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        byte[]  byte1 = new byte[]{1,2,2};
        BasicIntVector  re = (BasicIntVector)factory.createScalar(Entity.DATA_TYPE.DT_INT_ARRAY, byte1,0);
        assertEquals("[1,2,2]",re.getString());
    }
    @Test
    public void test_BasicEntityFactory_createScalar_createAnyVector_char() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        char[] char1 = new char[]{1,1,2};
        String ex = null;
        try{
            BasicIntVector  re = (BasicIntVector)factory.createScalar(Entity.DATA_TYPE.DT_INT, char1,0);
        }catch(Exception e){
            ex = e.getMessage();
        }
        assertEquals("Failed to insert data, only arrayVector support data vector for DT_INT.",ex);
    }
    @Test
    public void test_BasicEntityFactory_createScalar_createAnyVector_boolean() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        boolean[] bool1 = new boolean[]{true,false,false};
        String ex = null;
        try{
            BasicIntVector  re = (BasicIntVector)factory.createScalar(Entity.DATA_TYPE.DT_INT_ARRAY, bool1,0);
        }catch(Exception e){
            ex = e.getMessage();
        }
        assertEquals("Failed to insert data. Cannot convert boolean to DT_INT.",ex);
    }
    @Test
    public void test_BasicEntityFactory_createScalar_createAnyVector_long() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        long[] long1 = new long[]{-100l,0l,100l};
        BasicIntVector  re = (BasicIntVector)factory.createScalar(Entity.DATA_TYPE.DT_INT_ARRAY, long1,0);
        assertEquals("[-100,0,100]",re.getString());
    }
    @Test
    public void test_BasicEntityFactory_createScalar_LocalDate() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        LocalDate ld = LocalDate.of(2022,1,1);
        String ex = null;
        try{
            BasicTime  re = (BasicTime)factory.createScalar(Entity.DATA_TYPE.DT_SYMBOL, ld,0);
        }catch(Exception e){
            ex = e.getMessage();
        }
        assertEquals("Failed to insert data. Cannot convert LocalDate to DT_SYMBOL.",ex);
    }
    @Test
    public void test_BasicEntityFactory_createScalar_LocalDateTime() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        LocalDateTime ld = LocalDateTime.of(2022,1,1,1,1,1,10000);
        String ex = null;
        try{
            BasicTime  re = (BasicTime)factory.createScalar(Entity.DATA_TYPE.DT_SYMBOL, ld,0);
        }catch(Exception e){
            ex = e.getMessage();
        }
        assertEquals("Failed to insert data. Cannot convert LocalDateTime to DT_SYMBOL.",ex);
    }
    @Test
    public void test_BasicEntityFactory_createScalar_LocalTime() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        LocalTime t = LocalTime.of(10, 03, 10, 2030);
        String ex = null;
        try{
            BasicTime  re = (BasicTime)factory.createScalar(Entity.DATA_TYPE.DT_SYMBOL, t,0);
        }catch(Exception e){
            ex = e.getMessage();
        }
        assertEquals("Failed to insert data. Cannot convert LocalTime to DT_SYMBOL.",ex);
    }
    @Test
    public void test_BasicEntityFactory_createScalar_Calendar() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, 2022);
        calendar.set(Calendar.MONTH, Calendar.JANUARY);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 10);
        calendar.set(Calendar.MINUTE, 30);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 2);
        BasicTime  re = (BasicTime)factory.createScalar(Entity.DATA_TYPE.DT_TIME, calendar,0);
        assertEquals("10:30:00.002",re.getString());
    }
    @Test
    public void test_BasicEntityFactory_createScalar_boolean() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        String ex = null;
        try{
            BasicShort  re = (BasicShort)factory.createScalar(Entity.DATA_TYPE.DT_SYMBOL, (boolean)true,0);
        }catch(Exception e){
            ex = e.getMessage();
        }
        assertEquals("Failed to insert data. Cannot convert boolean to DT_SYMBOL.",ex);
    }
    @Test
    public void test_BasicEntityFactory_createScalar_char() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        String ex = null;
        try{
            BasicByteVector  re = (BasicByteVector)factory.createScalar(Entity.DATA_TYPE.DT_BYTE, (char)-129,1);
        }catch(Exception e){
            ex = e.getMessage();
        }
        assertEquals("Failed to insert data, char cannot be converted because it exceeds the range of DT_BYTE.",ex);
        String ex1 = null;
        try{
            BasicByteVector  re = (BasicByteVector)factory.createScalar(Entity.DATA_TYPE.DT_BYTE, (char)129,1);
        }catch(Exception e){
            ex1 = e.getMessage();
        }
        assertEquals("Failed to insert data, char cannot be converted because it exceeds the range of DT_BYTE.",ex1);

    }
    @Test
    public void test_BasicEntityFactory_createScalar_byte() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        String ex = null;
        try{
            BasicShort  re = (BasicShort)factory.createScalar(Entity.DATA_TYPE.DT_SYMBOL, (byte)1,0);
        }catch(Exception e){
            ex = e.getMessage();
        }
        assertEquals("Failed to insert data. Cannot convert byte to DT_SYMBOL.",ex);
    }
    @Test
    public void test_BasicEntityFactory_createScalar_short() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        String ex = null;
        try{
            BasicByteVector  re = (BasicByteVector)factory.createScalar(Entity.DATA_TYPE.DT_BYTE, (short)-129,1);
        }catch(Exception e){
            ex = e.getMessage();
        }
        assertEquals("Failed to insert data, short cannot be converted because it exceeds the range of DT_BYTE.",ex);
        String ex1 = null;
        try{
            BasicByteVector  re = (BasicByteVector)factory.createScalar(Entity.DATA_TYPE.DT_BYTE, (short)129,1);
        }catch(Exception e){
            ex1 = e.getMessage();
        }
        assertEquals("Failed to insert data, short cannot be converted because it exceeds the range of DT_BYTE.",ex1);
        String ex2 = null;
        try{
            BasicShort  re = (BasicShort)factory.createScalar(Entity.DATA_TYPE.DT_SYMBOL, (short)129,0);
        }catch(Exception e){
            ex2 = e.getMessage();
        }
        assertEquals("Failed to insert data. Cannot convert short to DT_SYMBOL.",ex2);
    }
    @Test
    public void test_BasicEntityFactory_createScalar_string() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        String ex = null;
        BasicDecimal128  re1 = (BasicDecimal128)factory.createScalar(Entity.DATA_TYPE.DT_DECIMAL128, "-21474836",2);
        assertEquals("-21474836.00",re1.getString());
        BasicDecimal128  re2 = (BasicDecimal128)factory.createScalar(Entity.DATA_TYPE.DT_DECIMAL128, "21474836",2);
        assertEquals("21474836.00",re2.getString());
    }

    @Test
    public void test_BasicEntityFactory_createScalar_float() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        String ex = null;
        BasicDecimal32  re7 = (BasicDecimal32)factory.createScalar(Entity.DATA_TYPE.DT_DECIMAL32, (float)-214748360,0);
        assertEquals("-214748352",re7.getString());
        BasicDecimal32  re8 = (BasicDecimal32)factory.createScalar(Entity.DATA_TYPE.DT_DECIMAL32, (float)21474836,0);
        assertEquals("21474836",re8.getString());

        BasicDecimal64  re9 = (BasicDecimal64)factory.createScalar(Entity.DATA_TYPE.DT_DECIMAL64, (float)-21474836,2);
        assertEquals("-21474836.00",re9.getString());
        BasicDecimal64  re10 = (BasicDecimal64)factory.createScalar(Entity.DATA_TYPE.DT_DECIMAL64, (float)21474836,2);
        assertEquals("21474836.00",re10.getString());

        try{
            BasicString  re = (BasicString)factory.createScalar(Entity.DATA_TYPE.DT_SYMBOL, (float)2147483647,0);
        }catch(Exception e){
            ex = e.getMessage();
        }
        assertEquals("Failed to insert data. Cannot convert float to DT_SYMBOL.",ex);
    }
    @Test
    public void test_BasicEntityFactory_createScalar_double() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        String ex = null;
        try{
            BasicFloat  re = (BasicFloat)factory.createScalar(Entity.DATA_TYPE.DT_FLOAT, (double)(-440282350000000000000000000000000000002.0),1);
        }catch(Exception e){
            ex = e.getMessage();
        }
        assertEquals("Failed to insert data, double cannot be converted because it exceeds the range of DT_FLOAT.",ex);
        String ex1 = null;
        try{
            BasicFloat  re = (BasicFloat)factory.createScalar(Entity.DATA_TYPE.DT_FLOAT, (double)(440282350000000000000000000000000000002.0),1);
        }catch(Exception e){
            ex1 = e.getMessage();
        }
        assertEquals("Failed to insert data, double cannot be converted because it exceeds the range of DT_FLOAT.",ex1);

        BasicFloat  re1 = (BasicFloat)factory.createScalar(Entity.DATA_TYPE.DT_FLOAT, (double)122.222,1);
        assertEquals("122.22200012",re1.getString());

        BasicFloat  re2 = (BasicFloat)factory.createScalar(Entity.DATA_TYPE.DT_FLOAT, (double)-144444,1);
        assertEquals("-144444",re2.getString());

        BasicDecimal32  re7 = (BasicDecimal32)factory.createScalar(Entity.DATA_TYPE.DT_DECIMAL32, (double)-2147483647,0);
        assertEquals("-2147483647",re7.getString());
        BasicDecimal32  re8 = (BasicDecimal32)factory.createScalar(Entity.DATA_TYPE.DT_DECIMAL32, (double)2147483647,0);
        assertEquals("2147483647",re8.getString());

        BasicDecimal64  re9 = (BasicDecimal64)factory.createScalar(Entity.DATA_TYPE.DT_DECIMAL64, (double)-2147483647,2);
        assertEquals("-2147483647.00",re9.getString());
        BasicDecimal64  re10 = (BasicDecimal64)factory.createScalar(Entity.DATA_TYPE.DT_DECIMAL64, (double)2147483647,2);
        assertEquals("2147483647.00",re10.getString());
        String ex2 = null;
        try{
            BasicString  re = (BasicString)factory.createScalar(Entity.DATA_TYPE.DT_SYMBOL, (double)2147483647,0);
        }catch(Exception e){
            ex2 = e.getMessage();
        }
        assertEquals("Failed to insert data. Cannot convert double to DT_SYMBOL.",ex2);
    }
    @Test
    public void test_BasicEntityFactory_createScalar_int() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        String ex = null;
        try{
            BasicByteVector  re = (BasicByteVector)factory.createScalar(Entity.DATA_TYPE.DT_BYTE, -129,1);
        }catch(Exception e){
            ex = e.getMessage();
        }
        assertEquals("Failed to insert data, int cannot be converted because it exceeds the range of DT_BYTE.",ex);
        String ex1 = null;
        try{
            BasicByteVector  re = (BasicByteVector)factory.createScalar(Entity.DATA_TYPE.DT_BYTE, 129,1);
        }catch(Exception e){
            ex1 = e.getMessage();
        }
        assertEquals("Failed to insert data, int cannot be converted because it exceeds the range of DT_BYTE.",ex1);
        String ex2 = null;
        try{
            BasicByteVector  re = (BasicByteVector)factory.createScalar(Entity.DATA_TYPE.DT_SHORT, -32769,1);
        }catch(Exception e){
            ex2 = e.getMessage();
        }
        assertEquals("Failed to insert data, int cannot be converted because it exceeds the range of DT_SHORT.",ex2);
        String ex3 = null;
        try{
            BasicByteVector  re = (BasicByteVector)factory.createScalar(Entity.DATA_TYPE.DT_SHORT, 32768,1);
        }catch(Exception e){
            ex3 = e.getMessage();
        }
        assertEquals("Failed to insert data, int cannot be converted because it exceeds the range of DT_SHORT.",ex3);

        BasicDate  re1 = (BasicDate)factory.createScalar(Entity.DATA_TYPE.DT_DATE, 1,1);
        assertEquals("1970.01.02",re1.getString());

        BasicMonth  re2 = (BasicMonth)factory.createScalar(Entity.DATA_TYPE.DT_MONTH, 1,1);
        assertEquals("0000.02M",re2.getString());

        BasicTime  re3 = (BasicTime)factory.createScalar(Entity.DATA_TYPE.DT_TIME, 1,1);
        assertEquals("00:00:00.001",re3.getString());

        BasicSecond  re4 = (BasicSecond)factory.createScalar(Entity.DATA_TYPE.DT_SECOND, 1,1);
        assertEquals("00:00:01",re4.getString());

        BasicMinute  re5 = (BasicMinute)factory.createScalar(Entity.DATA_TYPE.DT_MINUTE, 1,1);
        assertEquals("00:01m",re5.getString());

        BasicDateHour  re6 = (BasicDateHour)factory.createScalar(Entity.DATA_TYPE.DT_DATEHOUR, 1,1);
        assertEquals("1970.01.01T01",re6.getString());

        BasicDecimal32  re7 = (BasicDecimal32)factory.createScalar(Entity.DATA_TYPE.DT_DECIMAL32, -2147483647,0);
        assertEquals("-2147483647",re7.getString());
        BasicDecimal32  re8 = (BasicDecimal32)factory.createScalar(Entity.DATA_TYPE.DT_DECIMAL32, 2147483647,0);
        assertEquals("2147483647",re8.getString());

        BasicDecimal64  re9 = (BasicDecimal64)factory.createScalar(Entity.DATA_TYPE.DT_DECIMAL64, -2147483647,2);
        assertEquals("-2147483647.00",re9.getString());
        BasicDecimal64  re10 = (BasicDecimal64)factory.createScalar(Entity.DATA_TYPE.DT_DECIMAL64, 2147483647,2);
        assertEquals("2147483647.00",re10.getString());

        BasicDecimal128  re11 = (BasicDecimal128)factory.createScalar(Entity.DATA_TYPE.DT_DECIMAL128, -2147483647,2);
        //assertEquals("-2147483647.00",re11.getString());//AJ-586
        BasicDecimal128  re12 = (BasicDecimal128)factory.createScalar(Entity.DATA_TYPE.DT_DECIMAL128, 2147483647,2);
        //assertEquals("2147483647.00",re12.getString());//AJ-586
        String ex4 = null;
        try{
            BasicString  re = (BasicString)factory.createScalar(Entity.DATA_TYPE.DT_SYMBOL, 2147483647,0);
        }catch(Exception e){
            ex4 = e.getMessage();
        }
        assertEquals("Failed to insert data. Cannot convert int to DT_SYMBOL.",ex4);
    }
    @Test
    public void test_BasicEntityFactory_createScalar_long() throws Exception {
        BasicEntityFactory factory = new BasicEntityFactory();
        String ex = null;
        try{
            BasicByteVector  re = (BasicByteVector)factory.createScalar(Entity.DATA_TYPE.DT_BYTE, -129l,1);
        }catch(Exception e){
            ex = e.getMessage();
        }
        assertEquals("Failed to insert data, long cannot be converted because it exceeds the range of DT_BYTE.",ex);
        String ex1 = null;
        try{
            BasicByteVector  re = (BasicByteVector)factory.createScalar(Entity.DATA_TYPE.DT_BYTE, 129l,1);
        }catch(Exception e){
            ex1 = e.getMessage();
        }
        assertEquals("Failed to insert data, long cannot be converted because it exceeds the range of DT_BYTE.",ex1);
        String ex2 = null;
        try{
            BasicByteVector  re = (BasicByteVector)factory.createScalar(Entity.DATA_TYPE.DT_SHORT, -32769l,1);
        }catch(Exception e){
            ex2 = e.getMessage();
        }
        assertEquals("Failed to insert data, long cannot be converted because it exceeds the range of DT_SHORT.",ex2);
        String ex3 = null;
        try{
            BasicByteVector  re = (BasicByteVector)factory.createScalar(Entity.DATA_TYPE.DT_SHORT, 32768l,1);
        }catch(Exception e){
            ex3 = e.getMessage();
        }
        assertEquals("Failed to insert data, long cannot be converted because it exceeds the range of DT_SHORT.",ex3);
        String ex4 = null;
        try{
            BasicByteVector  re = (BasicByteVector)factory.createScalar(Entity.DATA_TYPE.DT_INT, -2147483649l,1);
        }catch(Exception e){
            ex4 = e.getMessage();
        }
        assertEquals("Failed to insert data, long cannot be converted because it exceeds the range of DT_INT.",ex4);
        try{
            BasicByteVector  re = (BasicByteVector)factory.createScalar(Entity.DATA_TYPE.DT_INT, 2147483648l,1);
        }catch(Exception e){
            ex = e.getMessage();
        }
        assertEquals("Failed to insert data, long cannot be converted because it exceeds the range of DT_INT.",ex);

        BasicNanoTime  re1 = (BasicNanoTime)factory.createScalar(Entity.DATA_TYPE.DT_NANOTIME, 9999999999l,1);
        assertEquals("00:00:09.999999999",re1.getString());

        BasicNanoTimestamp  re2 = (BasicNanoTimestamp)factory.createScalar(Entity.DATA_TYPE.DT_NANOTIMESTAMP, 9999999999l,1);
        assertEquals("1970.01.01T00:00:09.999999999",re2.getString());

        BasicDecimal32  re3 = (BasicDecimal32)factory.createScalar(Entity.DATA_TYPE.DT_DECIMAL32, -2147483647l,0);
        assertEquals("-2147483647",re3.getString());
        BasicDecimal32  re4 = (BasicDecimal32)factory.createScalar(Entity.DATA_TYPE.DT_DECIMAL32, 2147483647l,0);
        assertEquals("2147483647",re4.getString());
        String ex5 = null;
        try{
            BasicByteVector  re = (BasicByteVector)factory.createScalar(Entity.DATA_TYPE.DT_DECIMAL32, -2147483649l,1);
        }catch(Exception e){
            ex5 = e.getMessage();
        }
        assertEquals("Failed to insert data, long cannot be converted because it exceeds the range of DT_DECIMAL32.",ex5);
        String ex6 = null;
        try{
            BasicByteVector  re = (BasicByteVector)factory.createScalar(Entity.DATA_TYPE.DT_DECIMAL32, 2147483649l,1);
        }catch(Exception e){
            ex6 = e.getMessage();
        }
        assertEquals("Failed to insert data, long cannot be converted because it exceeds the range of DT_DECIMAL32.",ex6);

        BasicDecimal64  re5 = (BasicDecimal64)factory.createScalar(Entity.DATA_TYPE.DT_DECIMAL64, -2147483647l,2);
        assertEquals("-2147483647.00",re5.getString());
        BasicDecimal64  re6 = (BasicDecimal64)factory.createScalar(Entity.DATA_TYPE.DT_DECIMAL64, 2147483647l,2);
        assertEquals("2147483647.00",re6.getString());
        String ex7 = null;
        try{
            BasicDecimal64  re7 = (BasicDecimal64)factory.createScalar(Entity.DATA_TYPE.DT_SYMBOL, 2147483647l,0);
        }catch(Exception e){
            ex7 = e.getMessage();
        }
        assertEquals("Failed to insert data. Cannot convert long to DT_SYMBOL.",ex7);
    }
}
