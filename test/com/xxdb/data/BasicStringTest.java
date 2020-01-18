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

public class BasicStringTest {
    private  DBConnection conn;
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
    public void test_getHash() throws IOException{
        List<Integer> num = Arrays.asList(13,43,71,97,4097);
        BasicStringVector v = new BasicStringVector(6);
        v.setString(0,"!@#$%^&*()");
        v.setString(1,"我是中文测试内容");
        v.setString(2,"我是!@#$%^中文&*()");
        v.setString(3,"e1281ls.zxl.d.,cxnv./';'sla");
        v.setString(4,"abckdlskdful");
        v.setNull(5);
        for(int b : num){
            for(int i=0;i<v.rows();i++){
                int expected = getServerHash(v.get(i),b);
                Assert.assertEquals(expected, v.hashBucket(i, b));
                Assert.assertEquals(expected, v.get(i).hashBucket(b));
            }
        }
    }
}
