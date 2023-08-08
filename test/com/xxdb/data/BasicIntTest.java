package com.xxdb.data;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.DBConnection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.*;

public class BasicIntTest {
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
    private int getServerHash(Scalar s, int bucket) throws IOException{
        List<Entity> args = new ArrayList<>();
        args.add(s);
        args.add(new BasicInt(bucket));
        BasicInt re = (BasicInt)conn.run("hashBucket",args);
        return re.getInt();
    }

    @Test
    public void test_getHash() throws  IOException{
        List<Integer> num = Arrays.asList(13,43,71,97,4097);
        BasicIntVector v = new BasicIntVector(6);
        v.setInt(0,2147483647);
        v.setInt(1,-2147483647);
        v.setInt(2,99);
        v.setInt(3,0);
        v.setInt(4,-12);
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
    public void TestCombineIntVector() throws Exception {
        int[] data = {4, 5, 3, 6};
        BasicIntVector v = new BasicIntVector(data );
        int[] data2 = { 2, 5, 1};
        BasicIntVector vector2 = new BasicIntVector(data2 );
        BasicIntVector res= (BasicIntVector) v.combine(vector2);
        int[] datas = {4, 5, 3, 6, 2, 5, 1};
        for (int i=0;i<res.rows();i++){
            assertEquals(datas[i],((Scalar)res.get(i)).getNumber());
        }
        assertEquals(7,res.rows());
    }

    @Test
    public void test_BasicInt() throws Exception {
        BasicInt bi = new BasicInt(Integer.MIN_VALUE);
        assertNotNull(bi.getNumber());
        assertFalse(bi.equals(null));
    }

    @Test(expected = Exception.class)
    public void test_BasicIntMatrix_ListNull() throws Exception {
        BasicIntMatrix bim = new BasicIntMatrix(1,1,null);
    }

    @Test(expected = Exception.class)
    public void test_BasicIntMatrix_arrayNull() throws Exception {
        List<int[]> list = new ArrayList<>();
        list.add(new int[]{8,11});
        list.add(null);
        BasicIntMatrix bim = new BasicIntMatrix(2,2,list);
    }

    @Test
    public void test_BasicIntMatrix() throws Exception {
        List<int[]> list = new ArrayList<>();
        list.add(new int[]{1,4,5});
        list.add(new int[]{Integer.MIN_VALUE,0,Integer.MAX_VALUE});
        list.add(new int[]{5,2,1});
        BasicIntMatrix bim = new BasicIntMatrix(3,3,list);
        assertTrue(bim.isNull(0,1));
        assertEquals(Entity.DATA_CATEGORY.INTEGRAL,bim.getDataCategory());
        assertEquals(BasicInt.class,bim.getElementClass());
        bim.setNull(1,1);
        assertTrue(bim.isNull(1,1));
    }

    @Test
    public void test_BasicIntVector(){
        List<Integer> list = new ArrayList<>();
        list.add(5);
        list.add(7);
        list.add(8);
        list.add(Integer.MIN_VALUE);
        list.add(null);
        BasicIntVector biv = new BasicIntVector(list);
        assertEquals(BasicInt.class,biv.getElementClass());
        assertEquals(new BasicInt(7),biv.get(1));
        assertEquals(Entity.DATA_CATEGORY.INTEGRAL,biv.getDataCategory());
        assertEquals("[5,7,8,,,5,7,8]",biv.getSubVector(new int[]{0,1,2,3,4,0,1,2}).getString());
    }

    @Test
    public void test_BasicIntVector_wvtb() throws IOException {
        List<Integer> list = new ArrayList<>();
        list.add(5);
        list.add(7);
        list.add(8);
        list.add(Integer.MIN_VALUE);
        BasicIntVector biv = new BasicIntVector(list);
        ByteBuffer bb = biv.writeVectorToBuffer(ByteBuffer.allocate(16));
        assertEquals("[0, 0, 0, 5, 0, 0, 0, 7, 0, 0, 0, 8, -128, 0, 0, 0]",Arrays.toString(bb.array()));
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicIntVector_asof_error(){
        List<Integer> list = new ArrayList<>();
        list.add(5);
        list.add(7);
        list.add(8);
        list.add(Integer.MIN_VALUE);
        BasicIntVector biv = new BasicIntVector(list);
        biv.asof(new BasicComplex(1.9,8.5));
    }

    @Test
    public void test_BasicIntVector_asof_normal(){
        List<Integer> list = new ArrayList<>();
        list.add(5);
        list.add(7);
        list.add(8);
        list.add(Integer.MIN_VALUE);
        BasicIntVector biv = new BasicIntVector(list);
        assertEquals(3,biv.asof(new BasicInt(12)));
        assertEquals(0,biv.asof(new BasicInt(6)));
    }

    @Test
    public void test_BasicIntVector_Append() throws Exception {
        BasicIntVector biv = new BasicIntVector(new int[]{5,11,23});
        int size = biv.size;
        int capacity = biv.capaticy;
        biv.Append(new BasicInt(12));
        assertEquals(size+1,biv.rows());
        assertEquals(capacity*2,biv.capaticy);
        biv.Append(new BasicIntVector(new int[]{40,21,33}));
        assertEquals(size+4,biv.size);
        assertEquals(capacity*2+3,biv.capaticy);
    }
    @Test
    public void test_intValue() throws Exception {
        BasicInt bb = new BasicInt(1234);
        bb.setNull();
        assertEquals(null,bb.intValue());
        BasicInt bb1 = new BasicInt(1234);
        assertEquals("1234",bb1.intValue().toString());
    }
    @Test
    public void test_BasicInt_toJSONString() throws Exception {
        BasicInt bi = new BasicInt(Integer.MIN_VALUE);
        String re = JSONObject.toJSONString(bi);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"INTEGRAL\",\"dataForm\":\"DF_SCALAR\",\"dataType\":\"DT_INT\",\"dictionary\":false,\"int\":-2147483648,\"jsonString\":\"null\",\"matrix\":false,\"null\":true,\"number\":-2147483648,\"pair\":false,\"scalar\":true,\"scale\":0,\"string\":\"\",\"table\":false,\"vector\":false}", re);
    }
    @Test
    public void test_BasicIntVector_toJSONString() throws Exception {
        BasicIntVector biv = new BasicIntVector(new int[]{5,11,23});
        String re = JSONObject.toJSONString(biv);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataArray\":[5,11,23],\"dataCategory\":\"INTEGRAL\",\"dataForm\":\"DF_VECTOR\",\"dataType\":\"DT_INT\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicInt\",\"matrix\":false,\"pair\":false,\"scalar\":false,\"string\":\"[5,11,23]\",\"table\":false,\"unitLength\":4,\"vector\":true}", re);
    }
    @Test
    public void test_BasicIntMatrix_toJSONString() throws Exception {
        List<int[]> list = new ArrayList<>();
        list.add(new int[]{1,4,5});
        list.add(new int[]{Integer.MIN_VALUE,0,Integer.MAX_VALUE});
        list.add(new int[]{5,2,1});
        BasicIntMatrix bim = new BasicIntMatrix(3,3,list);
        String re = JSONObject.toJSONString(bim);
        System.out.println(re);
        assertEquals("{\"chart\":false,\"chunk\":false,\"dataCategory\":\"INTEGRAL\",\"dataForm\":\"DF_MATRIX\",\"dataType\":\"DT_INT\",\"dictionary\":false,\"elementClass\":\"com.xxdb.data.BasicInt\",\"matrix\":true,\"pair\":false,\"scalar\":false,\"string\":\"#0 #1         #2\\n1             5 \\n4  0          2 \\n5  2147483647 1 \\n\",\"table\":false,\"vector\":false}", re);
    }

}
