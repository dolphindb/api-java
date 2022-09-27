package com.xxdb.data;

import com.xxdb.DBConnection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.*;

public class BasicLongTest {
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
        BasicLongVector v = new BasicLongVector(6);
        v.setLong(0,9223372036854775807l);
        v.setLong(1,-9223372036854775807l);
        v.setLong(2,12);
        v.setLong(3,0);
        v.setLong(4,-12);
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
    public void TestCombineLongVector() throws Exception {
        Long[] data = {1l,-1l,3l};
        BasicLongVector v = new BasicLongVector(Arrays.asList(data));
        Long[] data2 = {1l,-1l,3l,9l};
        BasicLongVector vector2 = new BasicLongVector( Arrays.asList(data2));
        BasicLongVector res= (BasicLongVector) v.combine(vector2);
        Long[] datas = {1l,-1l,3l,1l,-1l,3l,9l};
        for (int i=0;i<res.rows();i++){
            assertEquals(datas[i],((Scalar)res.get(i)).getNumber());

        }
        assertEquals(7,res.rows());
    }

    @Test
    public void test_BasicLong() throws Exception {
        BasicLong bl = new BasicLong(9000L);
        assertEquals("9000",bl.getJsonString());
        assertTrue(new BasicLong(Long.MIN_VALUE).isNull());
        assertEquals("",new BasicLong(Long.MIN_VALUE).getString());
        assertFalse(bl.equals(null));
    }

    @Test
    public void test_BasicLongMatrix(){
        BasicLongMatrix blm = new BasicLongMatrix(2,2);
        blm.setLong(0,0,Long.MAX_VALUE);
        blm.setLong(0,1,7420L);
        blm.setLong(1,0,9820L);
        blm.setLong(1,1,Long.MIN_VALUE);
        assertEquals(Entity.DATA_CATEGORY.INTEGRAL,blm.getDataCategory());
        assertEquals(BasicLong.class,blm.getElementClass());
        assertTrue(blm.isNull(1,1));
        assertFalse(blm.isNull(1,0));
        blm.setNull(0,0);
        assertTrue(blm.isNull(0,0));
    }

    @Test(expected = Exception.class)
    public void test_BasicLongMatrix_listNull() throws Exception {
        BasicLongMatrix blm = new BasicLongMatrix(2,2,null);
    }

    @Test(expected = Exception.class)
    public void test_BasicLongMatrix_arrNull() throws Exception {
        List<long[]> list = new ArrayList<>();
        list.add(new long[]{7420L,9810L,9820L});
        list.add(new long[]{659L,810L,990L});
        list.add(null);
        BasicLongMatrix blm = new BasicLongMatrix(3,3,list);
    }

    @Test
    public void test_BasicLongVector(){
        List<Long> list = new ArrayList<>();
        list.add(855L);
        list.add(865L);
        list.add(null);
        list.add(888L);
        BasicLongVector blv = new BasicLongVector(list);
        assertEquals("888",blv.get(3).getString());
        assertEquals("[888,865,855]",blv.getSubVector(new int[]{3,1,0}).getString());
        assertEquals(Entity.DATA_CATEGORY.INTEGRAL,blv.getDataCategory());
        assertEquals(BasicLong.class,blv.getElementClass());
    }

    @Test
    public void test_BasicLongVector_wvtb() throws IOException {
        List<Long> list = new ArrayList<>();
        list.add(855L);
        list.add(865L);
        list.add(888L);
        BasicLongVector blv = new BasicLongVector(list);
        ByteBuffer bb = blv.writeVectorToBuffer(ByteBuffer.allocate(24));
        assertEquals("[0, 0, 0, 0, 0, 0, 3, 87, 0, 0, 0, 0, " +
                "0, 0, 3, 97, 0, 0, 0, 0, 0, 0, 3, 120]",Arrays.toString(bb.array()));
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicLongVector_asof_error(){
        List<Long> list = new ArrayList<>();
        list.add(855L);
        list.add(865L);
        list.add(888L);
        BasicLongVector blv = new BasicLongVector(list);
        blv.asof(new BasicComplex(5.75,7.37));
    }

    @Test
    public void test_BasicLongVector_asof_normal(){
        List<Long> list = new ArrayList<>();
        list.add(855L);
        list.add(865L);
        list.add(888L);
        BasicLongVector blv = new BasicLongVector(list);
        blv.asof(new BasicLong(860L));
    }

    @Test
    public void test_BasicLongVector_Append() throws Exception {
        BasicLongVector blv = new BasicLongVector(new long[]{600,615,617});
        int size = blv.size;
        int capacity = blv.capaticy;
        blv.Append(new BasicLong(625));
        assertEquals(size+1,blv.size);
        assertEquals(capacity*2,blv.capaticy);
        blv.Append(new BasicLongVector(new long[]{630,632,636}));
        assertEquals(size+4,blv.size);
        assertEquals(capacity*2+3,blv.capaticy);
    }
}
