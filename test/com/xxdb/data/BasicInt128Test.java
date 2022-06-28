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

public class BasicInt128Test {
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
        BasicInt128Vector v = new BasicInt128Vector(6);

        v.set(0,BasicInt128.fromString("4b7545dc735379254fbf804dec34977f"));
        v.set(1,BasicInt128.fromString("6f29ffbf80722c9fd386c6e48ca96340"));
        v.set(2,BasicInt128.fromString("dd92685907f08a99ec5f8235c15a1588"));
        v.set(3,BasicInt128.fromString("4f5387611b41d1385e272e6e866f862d"));
        v.set(4,BasicInt128.fromString("130d6d5a0536c99ac7f9a01363b107c0"));
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
    public void TestCombineInt128Vector() throws Exception {
        BasicInt128Vector v = new BasicInt128Vector(4);
        v.set(0,BasicInt128.fromString("4b7545dc735379254fbf804dec34977f"));
        v.set(1,BasicInt128.fromString("6f29ffbf80722c9fd386c6e48ca96340"));
        v.set(2,BasicInt128.fromString("dd92685907f08a99ec5f8235c15a1588"));
        v.set(3,BasicInt128.fromString("4f5387611b41d1385e272e6e866f862d"));
        BasicInt128Vector vector2 = new BasicInt128Vector(2 );
        vector2.set(0,BasicInt128.fromString("4f5387611b41d1385e272e6e866f862d"));
        vector2.set(1,BasicInt128.fromString("130d6d5a0536c99ac7f9a01363b107c0"));
        BasicInt128Vector res= (BasicInt128Vector) v.combine(vector2);
        BasicInt128Vector res128 = new BasicInt128Vector(6);
        res128.set(0,BasicInt128.fromString("4b7545dc735379254fbf804dec34977f"));
        res128.set(1,BasicInt128.fromString("6f29ffbf80722c9fd386c6e48ca96340"));
        res128.set(2,BasicInt128.fromString("dd92685907f08a99ec5f8235c15a1588"));
        res128.set(3,BasicInt128.fromString("4f5387611b41d1385e272e6e866f862d"));
        res128.set(4,BasicInt128.fromString("4f5387611b41d1385e272e6e866f862d"));
        res128.set(5,BasicInt128.fromString("130d6d5a0536c99ac7f9a01363b107c0"));
        for (int i=0;i<res.rows();i++){
            assertEquals(res128.get(i).toString(),res.get(i).toString());
        }
        assertEquals(6,res.rows());
    }
}
