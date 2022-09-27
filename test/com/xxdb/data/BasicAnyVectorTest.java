package com.xxdb.data;

import com.xxdb.DBConnection;
import com.xxdb.io.*;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Scanner;

import static org.junit.Assert.*;

public class BasicAnyVectorTest {
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));

    @Test(expected = UnsupportedOperationException.class)
    public void test_combine(){
        BasicAnyVector bav = new BasicAnyVector(4);
        assertEquals(Entity.DATA_CATEGORY.MIXED,bav.getDataCategory());
        assertEquals(Entity.DATA_TYPE.DT_ANY,bav.getDataType());
        assertEquals(Entity.class,bav.getElementClass());
        Vector v = new BasicPointVector(5);
        bav.combine(v);
    }

    @Test(expected = RuntimeException.class)
    public void test_asof(){
        BasicAnyVector bav = new BasicAnyVector(5);
        Scalar value = new BasicPoint(2.1,8.4);
        bav.asof(value);
    }

    @Test(expected = RuntimeException.class)
    public void test_getUnitlength(){
        BasicAnyVector bav = new BasicAnyVector(2);
        bav.getUnitLength();
    }

    @Test(expected = RuntimeException.class)
    public void test_serialize() throws IOException {
        BasicAnyVector bav = new BasicAnyVector(1);
        ExtendedDataOutput out = new BigEndianDataOutputStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                System.out.println(b);
            }
        });
        bav.serialize(0,1,out);
    }

    @Test(expected = RuntimeException.class)
    public void test_serialize2() throws IOException {
        BasicAnyVector bav = new BasicAnyVector(5);
        AbstractVector.NumElementAndPartial numElementAndPartial = new AbstractVector.NumElementAndPartial(12,24);
        ByteBuffer bb = ByteBuffer.allocate(10);
        bav.serialize(0,1,65534,numElementAndPartial,bb);
    }

    @Test
    public void test_BasicAnyVector_Entity() throws IOException {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT);
        Entity[] arr = new Entity[3];
        arr[0] = conn.run("x=[1 3 2];x;");
        arr[1] = conn.run("y=1..4;y;");
        arr[2] = conn.run("z=1..10$5:2;z;");
        BasicAnyVector bav = new BasicAnyVector(arr,true);
        String str = "(([1,3,2]),[1,2,3,4],#0 #1\n" +
                "1  6 \n" +
                "2  7 \n" +
                "3  8 \n" +
                "4  9 \n" +
                "5  10\n" +
                ")";
        assertEquals(str,bav.getString());
        assertEquals("[1,2,3,4]",bav.getEntity(1).getString());
        assertEquals(3,bav.rows());
        String str2 = "(#0 #1\n" +
                "1  6 \n" +
                "2  7 \n" +
                "3  8 \n" +
                "4  9 \n" +
                "5  10\n" +
                ",([1,3,2]))";
        assertEquals(str2,bav.getSubVector(new int[]{2,0}).getString());
        assertFalse(bav.isNull(2));
        bav.setEntity(2,conn.run("x=1..3;y=4..6;z=dict(x,y);z;"));
        assertEquals("{1,2,3}->{4,5,6}",bav.getEntity(2).getString());
        bav.setNull(1);
        assertTrue(bav.isNull(1));
        try{
            bav.get(2);
        }catch(RuntimeException re){
            assertEquals("The element of the vector is not a scalar object.",re.getMessage());
        }
    }

    @Test
    public void test_BasicAnyVector_scalar() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT);
        Entity[] arr = new Entity[12];
        for (int i = 0; i < 10; i++) {
            arr[i] = conn.run(""+i);
        }
        arr[10] = conn.run("11.11");
        arr[11] = conn.run("true");
        BasicAnyVector bav = new BasicAnyVector(arr,false);
        assertEquals("4",bav.get(4).getString());
        assertEquals("(0,1,2,3,4,5,6,7,8,9,...)",bav.getString());
        bav.set(11, (Scalar) conn.run("date(2022.08.01);"));
        assertEquals("2022.08.01",bav.get(11).getString());
    }

    @Test
    public void test_ExtendedData() throws IOException {
        ExtendedDataInput in = new ExtendedDataInput() {
            @Override
            public boolean isLittleEndian() {
                return false;
            }

            @Override
            public String readString() throws IOException {
                return null;
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
                return 4;
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
        System.out.println(in.readShort());
        BasicAnyVector bav = new BasicAnyVector(in);
        ExtendedDataOutput out = new LittleEndianDataOutputStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                System.out.println(b);
            }
        });
        bav.writeVectorToOutputStream(out);
    }

    @Test(expected = RuntimeException.class)
    public void test_basicAnyVector_Append() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT);
        Entity[] arr = new Entity[12];
        for (int i = 0; i < 10; i++) {
            arr[i] = conn.run(""+i);
        }
        arr[10] = conn.run("11.11");
        arr[11] = conn.run("true");
        BasicAnyVector bav = new BasicAnyVector(arr,false);
        assertEquals("4",bav.get(4).getString());
        assertEquals("(0,1,2,3,4,5,6,7,8,9,...)",bav.getString());
        bav.set(11, (Scalar) conn.run("date(2022.08.01);"));
        assertEquals("2022.08.01",bav.get(11).getString());
        bav.Append(new BasicInt(16));
    }

}
