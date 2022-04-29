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

public class BasicShortTest {
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
                int expected = getServerHash(v.get(i),b);
                Assert.assertEquals(expected, v.hashBucket(i, b));
                Assert.assertEquals(expected, v.get(i).hashBucket(b));
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
            assertEquals(datas[i],res.get(i).getNumber());

        }
        assertEquals(7,res.rows());
    }
}
