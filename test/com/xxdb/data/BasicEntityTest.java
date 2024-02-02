package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.io.*;
import org.junit.Assert;
import org.junit.Test;

import javax.management.RuntimeErrorException;

import static org.junit.Assert.*;
import java.io.IOException;
import java.io.OutputStream;
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
}
