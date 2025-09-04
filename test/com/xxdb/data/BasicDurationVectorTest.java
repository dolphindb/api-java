package com.xxdb.data;

import com.xxdb.DBConnection;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ResourceBundle;

import static org.junit.Assert.assertEquals;

public class BasicDurationVectorTest {
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));

    @Test
    public void test_BasicDurationVector() throws Exception {
        BasicDurationVector bdv = new BasicDurationVector(3);
        bdv.setNull(0);
        bdv.setNull(1);
        bdv.setNull(2);
        assertEquals(-2147483648,((Scalar)bdv.get(1)).getNumber());
        assertEquals(0,bdv.hashBucket(0,1));
        assertEquals(4,bdv.getUnitLength());
    }

    @Test
    public void test_BasicDurationVector_set() throws Exception {
        BasicDurationVector bbv = new BasicDurationVector(6,6);
        String re = null;
        try{
            bbv.set(0,1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("BasicDurationVector.set not implemented yet.", re);
    }

    @Test
    public void test_BasicDurationVector_add() throws Exception {
        BasicDurationVector bbv = new BasicDurationVector(1,0);
        String re = null;
        try{
            bbv.add(1);
        }catch(Exception e){
            re = e.getMessage();
        }
        assertEquals("BasicDurationVector.add not implemented yet.", re);
    }

    @Test
    public void test_BasicDurationVector_wvtb() throws Exception {
        BasicDurationVector bdv = new BasicDurationVector(2);
        bdv.set(0,new BasicDuration(Entity.DURATION.WEEK,5));
        bdv.set(1,new BasicDuration(Entity.DURATION.HOUR,2));
        ByteBuffer bb = bdv.writeVectorToBuffer(ByteBuffer.allocate(16));
        assertEquals("[0, 0, 0, 5, 0, 0, 0, 7, 0, 0, 0, 2, 0, 0, 0, 5]", Arrays.toString(bb.array()));
    }

    @Test
    public void test_BasicDurationVector_Append() throws Exception {
        BasicDurationVector bdv = new BasicDurationVector(2);
        bdv.set(0,new BasicDuration(Entity.DURATION.WEEK,5));
        bdv.set(1,new BasicDuration(Entity.DURATION.HOUR,2));
        int size = bdv.rows();
        System.out.println(size);
        bdv.Append(new BasicDuration(Entity.DURATION.NS,7));
        bdv.Append(new BasicDuration(Entity.DURATION.NS,8));
        assertEquals(size+2,bdv.rows());
        BasicDurationVector bdv2 = new BasicDurationVector(2);
        bdv2.set(0,new BasicDuration(Entity.DURATION.MONTH,6));
        bdv2.set(1,new BasicDuration(Entity.DURATION.SECOND,15));
        bdv.Append(bdv2);
        System.out.println(bdv.getString());
        assertEquals(size+4,bdv.rows());
    }
//    @Test
//    public void test_BasicDurationVector_Append_1() throws Exception {
//        BasicDurationVector bdv = new BasicDurationVector(0);
//        BasicDuration bdv2 = new BasicDuration(Entity.DURATION.MONTH,6);
//        bdv2.setNull();
//        bdv.Append(bdv2);
//        System.out.println(bdv.getString());
//    }

    @Test
    public void test_BasicDurationVector_1() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        BasicAnyVector bdv = (BasicAnyVector)conn.run("[duration(3XNYS),duration(3XNYS)]");
        System.out.println(bdv.getString());
        assertEquals("(3XNYS,3XNYS)",bdv.getString());
        System.out.println(((BasicDuration)bdv.get(0)).getDuration());
        assertEquals(3,((BasicDuration)bdv.get(0)).getDuration());
        conn.close();
    }
    @Test
    public void test_BasicDurationVector_2() throws Exception {
        BasicDurationVector bdv = new BasicDurationVector(2);
        bdv.set(0,new BasicDuration("XNYS",3));
        bdv.set(1,new BasicDuration("XNYS",0));
        bdv.Append(new BasicDuration("XNYS",-999999999));
        assertEquals("[3XNYS,0XNYS,-999999999XNYS]",bdv.getString());
        assertEquals("3XNYS",bdv.get(0).getString());
    }
    @Test
    public void test_BasicDurationVector_3() throws Exception {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        BasicDurationVector Vector1 = (BasicDurationVector) conn.run("-5AAAA:0AAAA");
        BasicDurationVector Vector2 = (BasicDurationVector) conn.run("pair(-5AAAA,0AAAA)");
        assertEquals("[-5AAAA,0AAAA]",Vector1.getString());
        assertEquals("[-5AAAA,0AAAA]",Vector2.getString());
        conn.close();
    }
}
