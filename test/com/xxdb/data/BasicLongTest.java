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

public class BasicLongTest {
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
        BasicLongVector v = new BasicLongVector(6);
        v.setLong(0,9223372036854775807l);
        v.setLong(1,-9223372036854775807l);
        v.setLong(2,12);
        v.setLong(3,0);
        v.setLong(4,-12);
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
    public void TestCombineLongVector() throws Exception {
        Long[] data = {1l,-1l,3l};
        BasicLongVector v = new BasicLongVector(Arrays.asList(data));
        Long[] data2 = {1l,-1l,3l,9l};
        BasicLongVector vector2 = new BasicLongVector( Arrays.asList(data2));
        BasicLongVector res= (BasicLongVector) v.combine(vector2);
        Long[] datas = {1l,-1l,3l,1l,-1l,3l,9l};
        for (int i=0;i<res.rows();i++){
            assertEquals(datas[i],res.get(i).getNumber());

        }
        assertEquals(7,res.rows());
    }
}
