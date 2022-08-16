package com.xxdb.data;

import com.xxdb.DBConnection;
import com.xxdb.io.Long2;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ResourceBundle;

import static org.junit.Assert.assertEquals;

public class BasicUuidTest {
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
    public void test_getHash() throws  Exception{
        List<Integer> num = Arrays.asList(13,43,71,97,4097);
        BasicUuidVector v = new BasicUuidVector(6);
        v.set(0,BasicUuid.fromString("d4350822-e074-e1f6-4340-95b3148629bd"));
        v.set(1,BasicUuid.fromString("1825c36f-092c-3ed5-e246-b289d57c89df"));
        v.set(2,BasicUuid.fromString("776e76f8-40b8-c4ee-b2ae-068d75393362"));
        v.set(3,BasicUuid.fromString("1783fcf9-3116-1d71-2d7b-6630d6792c94"));
        v.set(4,BasicUuid.fromString("72b58dc4-9962-f690-2210-cc3a80507573"));
        v.setNull(5);
        for(int b : num){
            for(int i=0;i<v.rows();i++){
                int expected = getServerHash(v.get(i),b);
                Assert.assertEquals(expected, v.hashBucket(i, b));
                Assert.assertEquals(expected, v.get(i).hashBucket(b));
            }
        }
    }
    @Test
    public void TestCombineUuidVector() throws Exception {
        BasicUuidVector v = new BasicUuidVector(4);
        v.set(0,BasicUuid.fromString("d4350822-e074-e1f6-4340-95b3148629bd"));
        v.set(1,BasicUuid.fromString("1825c36f-092c-3ed5-e246-b289d57c89df"));
        v.set(2,BasicUuid.fromString("776e76f8-40b8-c4ee-b2ae-068d75393362"));
        v.set(3,BasicUuid.fromString("1783fcf9-3116-1d71-2d7b-6630d6792c94"));
        BasicUuidVector vector2 = new BasicUuidVector(2 );
        vector2.set(0,BasicUuid.fromString("1783fcf9-3116-1d71-2d7b-6630d6792c94"));
        vector2.set(1,BasicUuid.fromString("72b58dc4-9962-f690-2210-cc3a80507573"));
        BasicUuidVector res= (BasicUuidVector) v.combine(vector2);
        BasicUuidVector res128 = new BasicUuidVector(6);
        res128.set(0,BasicUuid.fromString("d4350822-e074-e1f6-4340-95b3148629bd"));
        res128.set(1,BasicUuid.fromString("1825c36f-092c-3ed5-e246-b289d57c89df"));
        res128.set(2,BasicUuid.fromString("776e76f8-40b8-c4ee-b2ae-068d75393362"));
        res128.set(3,BasicUuid.fromString("1783fcf9-3116-1d71-2d7b-6630d6792c94"));
        res128.set(4,BasicUuid.fromString("1783fcf9-3116-1d71-2d7b-6630d6792c94"));
        res128.set(5,BasicUuid.fromString("72b58dc4-9962-f690-2210-cc3a80507573"));
        for (int i=0;i<res.rows();i++){
            assertEquals(res128.get(i).toString(),res.get(i).toString());
        }
        assertEquals(6,res.rows());
    }

    @Test
    public void test_BasicUUidVector_Basic(){
        List<Long2> list = new ArrayList<>();
        list.add(new Long2(9000L,600L));
        list.add(new Long2(888L,200L));
        list.add(new Long2(9000L,10L));
        BasicUuidVector buv = new BasicUuidVector(list);
        assertEquals(BasicUuid.class,buv.getElementClass());
        assertEquals("[00000000-0000-2328-0000-00000000000a,00000000-0000-2328-0000-000000000258,00000000-0000-0378-0000-0000000000c8]",buv.getSubVector(new int[]{2,0,1}).getString());
    }

    @Test
    public void test_BasicUUidVector_df(){
        BasicUuidVector buv = new BasicUuidVector(Entity.DATA_FORM.DF_VECTOR,2);
        System.out.println(buv.getString());
    }
}
