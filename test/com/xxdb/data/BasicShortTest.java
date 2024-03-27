package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ResourceBundle;

import static org.junit.Assert.*;

public class BasicShortTest {
    private DBConnection conn;
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    @Before
    public  void setUp(){
        conn = new DBConnection();
        try{
            if(!conn.connect(HOST,PORT,"admin","123456")){
                throw new IOException("Failed to connect to 2xdb server");
            }
        }catch(IOException ex){
            ex.printStackTrace();
        }
    }

    @After
    public void tearDown() throws Exception {
        conn.close();
    }
    private int getServerHash(Scalar s, int bucket) throws IOException {
        List<Entity> args = new ArrayList<>();
        args.add(s);
        args.add(new BasicInt(bucket));
        BasicInt re = (BasicInt)conn.run("hashBucket",args);
        return re.getInt();
    }

    @Test
    public void test_getHash() throws  IOException{
        List<Integer> num = Arrays.asList(13,43,71,97,4097);
        BasicShortVector v = new BasicShortVector(6);
        v.setShort(0,(short)32767);
        v.setShort(1,(short)-32767);
        v.setShort(2,(short)12);
        v.setShort(3,(short)0);
        v.setShort(4,(short)-12);
        v.setNull(5);
        for(int b : num){
            for(int i=0;i<v.rows();i++){
                int expected = getServerHash(((Scalar)v.get(i)),b);
                Assert.assertEquals(expected, v.hashBucket(i, b));
                Assert.assertEquals(expected, ((Scalar)v.get(i)).hashBucket(b));
            }
        }
    }

    @Test
    public void TestCombineShortVector() throws Exception {
        Short[] data = {1,-1,3};
        BasicShortVector v = new BasicShortVector(Arrays.asList(data));
        Short[] data2 = {1,-1,3,9};
        BasicShortVector vector2 = new BasicShortVector( Arrays.asList(data2));
        BasicShortVector res= (BasicShortVector) v.combine(vector2);
        Short[] datas = {1,-1,3,1,-1,3,9};
        for (int i=0;i<res.rows();i++){
            assertEquals(datas[i],((Scalar)res.get(i)).getNumber());

        }
        assertEquals(7,res.rows());
    }

    @Test
    public void test_isNull() throws Exception {
        BasicShort bs = new BasicShort(Short.MIN_VALUE);
        assertTrue(bs.isNull());
        assertEquals("",bs.getString());
        assertFalse(new BasicShort(Short.MAX_VALUE).equals(null));
        assertEquals("11",new BasicShort((short) 11).getJsonString());
    }

    @Test
    public void test_BasicShortMatrix_BasicFunctions(){
        BasicShortMatrix bsm = new BasicShortMatrix(2,2);
        bsm.setShort(0,0, (short) 7);
        bsm.setShort(0,1, (short) 3);
        bsm.setShort(1,0, (short) 19);
        bsm.setShort(1,1,Short.MAX_VALUE);
        assertEquals(7,bsm.getShort(0,0));
        assertEquals(new BasicShort(Short.MAX_VALUE),bsm.get(1,1));
        bsm.setNull(1,1);
        assertTrue(bsm.isNull(1,1));
        assertEquals(Entity.DATA_CATEGORY.INTEGRAL,bsm.getDataCategory());
        assertEquals(BasicShort.class,bsm.getElementClass());
    }

    @Test(expected = Exception.class)
    public void test_BasicShortMatrix_listNull() throws Exception {
        BasicShortMatrix bsm = new BasicShortMatrix(2,2,null);
    }

    @Test(expected = Exception.class)
    public void test_BasicShortMatrix_ArrayNull() throws Exception {
        List<short[]> list = new ArrayList<>();
        list.add(new short[]{3,6,9});
        list.add(new short[]{2,5,8});
        list.add(null);
        BasicShortMatrix bsm = new BasicShortMatrix(3,3,list);
    }

    @Test
    public void test_BasicShortVector_basicFunctions(){
        List<Short> list = new ArrayList<>();
        list.add((short) 2);
        list.add((short) 7);
        list.add(null);
        list.add((short) 4);
        BasicShortVector bsv = new BasicShortVector(list);

        assertEquals("[4,2,7]",bsv.getSubVector(new int[]{3,0,1}).getString());
        assertTrue(bsv.isNull(2));
        assertEquals((short)7,bsv.getShort(1));
        assertEquals(BasicShort.class,bsv.getElementClass());
    }

    @Test
    public void test_BasicShortVector_wvtb() throws IOException {
        List<Short> list = new ArrayList<>();
        list.add((short) 2);
        list.add((short) 7);
        list.add((short) 16);
        list.add((short) 4);
        BasicShortVector bsv = new BasicShortVector(list);
        ByteBuffer bb = bsv.writeVectorToBuffer(ByteBuffer.allocate(8));
        assertEquals("[0, 2, 0, 7, 0, 16, 0, 4]",Arrays.toString(bb.array()));
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicShortVector_asof_error(){
        List<Short> list = new ArrayList<>();
        list.add((short) 2);
        list.add((short) 7);
        list.add((short) 16);
        list.add((short) 24);
        BasicShortVector bsv = new BasicShortVector(list);
        bsv.asof(new BasicPoint(5.9,7.6));
    }

    @Test
    public void test_BasicShortVector_asof(){
        List<Short> list = new ArrayList<>();
        list.add((short) 2);
        list.add((short) 7);
        list.add((short) 16);
        list.add((short) 24);
        BasicShortVector bsv = new BasicShortVector(list);
        assertEquals(2,bsv.asof(new BasicShort((short) 20)));
    }

    @Test
    public void test_BasicShortVector_Append() throws Exception {
        BasicShortVector bsv = new BasicShortVector(new short[]{55,11,46});
        int size = bsv.rows();
        bsv.Append(new BasicShort((short) 88));
        assertEquals(size+1,bsv.rows());
        bsv.Append(new BasicShortVector(new short[]{16,23,11}));
        assertEquals(size+4,bsv.rows());
    }
    @Test
    public void test_BasicShortVector_setNull() throws Exception {
        List<Short> list = new ArrayList<>();
        list.add((short) 2);
        list.add((short) 7);
        list.add((short) 16);
        BasicShortVector bsv = new BasicShortVector(list);
        bsv.setNull(0);
        assertEquals(true,bsv.isNull(0));
        //List<Short> list1 = null;
        //BasicShortVector bsv1 = new BasicShortVector(list1);
    }
    @Test
    public void test_shortValue() throws Exception {
        BasicShort bb = new BasicShort((short) 88);
        bb.setNull();
        assertEquals(null,bb.shortValue());
        BasicShort bb1 = new BasicShort((short) 88);
        assertEquals("88",bb1.shortValue().toString());
    }
    @Test
    public void test_BasicShort_toJSONString() throws Exception {
        BasicShort bb1 = new BasicShort((short) 88);
        String re = JSONObject.toJSONString(bb1);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"INTEGRAL\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_SHORT\",\"dictionary\":false,\"jsonString\":\"88\",\"matrix\":false,\"null\":false,\"number\":88,\"pair\":false,\"scalar\":true,\"short\":88,\"string\":\"88\",\"table\":false,\"vector\":false}", re);
    }
    @Test
    public void test_BasicShortVector_toJSONString() throws Exception {
        BasicShortVector bsv = new BasicShortVector(new short[]{55,11,46});
        String re = JSONObject.toJSONString(bsv);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[55,11,46],\"dataCategory\":\"INTEGRAL\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_SHORT\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicShort\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[55,11,46]\",\"table\":false,\"unitLength\":2,\"vector\":true}", re);
    }
    @Test
    public void test_BasicShortMatrix_toJSONString() throws Exception {
        BasicShortMatrix bsm = new BasicShortMatrix(2,2);
        String re = JSONObject.toJSONString(bsm);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"INTEGRAL\",\"dataForm\":\"DF_MATRIX\",\"dataType\":\"DT_SHORT\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicShort\",\"matrix\":true,\"pair\":false,\"scalar\":false,\"string\":\"#0 #1\\n0  0 \\n0  0 \\n\",\"table\":false,\"vector\":false}", re);
    }
}
