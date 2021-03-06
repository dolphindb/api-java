package com.xxdb.data;

import com.xxdb.DBConnection;
import com.xxdb.DBConnectionTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class BasicByteTest {
    private DBConnection conn;
    @Before
    public  void setUp(){
        conn = new DBConnection();
        try{
            if(!conn.connect(DBConnectionTest.HOST,DBConnectionTest.PORT,"admin","123456")){
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
        BasicByteVector v = new BasicByteVector(6);
        v.setByte(0,(byte)127);
        v.setByte(1,(byte)-127);
        v.setByte(2,(byte)12);
        v.setByte(3,(byte)0);
        v.setByte(4,(byte)-128);
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
    public void TestCombineByteVector() throws Exception {
        Byte[] data = {'4', '5', '3', '6'};
        BasicByteVector v = new BasicByteVector(Arrays.asList(data));
        Byte[] data2 = { '2', '5', '1'};
        BasicByteVector vector2 = new BasicByteVector(Arrays.asList(data2));
        BasicByteVector res= (BasicByteVector) v.combine(vector2);
        Byte[] datas = {'4', '5', '3', '6', '2', '5', '1'};
        for (int i=0;i<res.rows();i++){
            assertEquals(datas[i],res.get(i).getNumber());

        }
        assertEquals(7,res.rows());
    }
}
