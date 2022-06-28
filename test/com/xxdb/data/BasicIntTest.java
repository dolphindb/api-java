package com.xxdb.data;

import com.xxdb.DBConnection;
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
                int expected = getServerHash(v.get(i),b);
                Assert.assertEquals(expected, v.hashBucket(i, b));
                Assert.assertEquals(expected, v.get(i).hashBucket(b));
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
            assertEquals(datas[i],res.get(i).getNumber());
        }
        assertEquals(7,res.rows());
    }
}
