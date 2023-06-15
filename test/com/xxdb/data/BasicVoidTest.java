package com.xxdb.data;

import com.xxdb.DBConnection;
import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.io.*;
import com.xxdb.multithreadedtablewriter.MultithreadedTableWriter;
import org.apache.commons.lang3.ObjectUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.*;

public class BasicVoidTest {
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
    public void test_BasicVoidVector() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        Entity aa = (BasicInt)conn.run("1");
        BasicVoidVector v = new BasicVoidVector(6);
        v.setNull(0);
        Assert.assertEquals(true,v.isNull(0));
        Assert.assertEquals(true,v.isNull(1));
        Assert.assertEquals("",v.get(0).getString());
        v.set(2, aa);
        System.out.println(aa.getString());
        Assert.assertEquals("",v.get(2).getString());
    }
    @Test
    public void test_BasicVoidVector_1() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        Entity aa = (BasicInt)conn.run("1");
        ExtendedDataInput in = new BigEndianDataInputStream(new InputStream() {
            @Override
            public int read() throws IOException {
                return 6;
            }
        });
        BasicVoidVector v = new BasicVoidVector(Entity.DATA_FORM.DF_VECTOR,in);
        Assert.assertEquals("[]",v.getString());
    }

    @Test
    public void test_BasicVoidVector_deserialize() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        Entity aa = (BasicInt)conn.run("1");
        BasicVoidVector v = new BasicVoidVector(6);
        String str1 = v.getString();
        System.out.println(str1);
        ExtendedDataInput in = new BigEndianDataInputStream(new InputStream() {
            @Override
            public int read() throws IOException {
                return 6;
            }
        });
        v.deserialize(0,1,in);
        String str2 = v.getString();
        System.out.println(str2);
    }
    @Test
    public void test_BasicVoidVector_writeVectorToOutputStream() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        Entity aa = (BasicInt)conn.run("1");
        BasicVoidVector v = new BasicVoidVector(6);
        String str1 = v.getString();
        System.out.println(str1);
        ExtendedDataOutput out = new BigEndianDataOutputStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                System.out.println(b);
            }
        });
        v.writeVectorToOutputStream(out);
        String str2 = v.getString();
        System.out.println(str2);
    }
    @Test
    public void test_BasicVoidVector_serialize() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        Entity aa = (BasicInt)conn.run("1");
        BasicVoidVector v = new BasicVoidVector(6);
        String str1 = v.getString();
        System.out.println(str1);
        ExtendedDataOutput out = new BigEndianDataOutputStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                System.out.println(b);
            }
        });
        v.serialize(0,1, out);
        String str2 = v.getString();
        System.out.println(str2);
    }
    @Test
    public void test_BasicVoidVector_serialize_1() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        Entity aa = (BasicInt)conn.run("1");
        BasicVoidVector v = new BasicVoidVector(6);
        String str1 = v.getString();
        System.out.println(str1);
        ByteBuffer bb = ByteBuffer.allocate(20);
        System.out.println(bb.remaining());
        AbstractVector.NumElementAndPartial numElementAndPartial = new AbstractVector.NumElementAndPartial(30,2);
        int m = v.serialize(0,0,4,numElementAndPartial,bb);
        assertEquals(4,m);
        String str2 = v.getString();
        assertEquals("[,,,,,]",str2);
    }
    @Test
    public void test_BasicVoidVector_getSubVector() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        Entity aa = (BasicInt)conn.run("1");
        BasicVoidVector v = new BasicVoidVector(6);
        v.setNull(0);
        v.getSubVector(new int[]{0,5,4});
        String str2 = v.getString();
        System.out.println(str2);
    }
    @Test
    public void test_BasicVoidVector_combine() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        BasicVoidVector v = new BasicVoidVector(6);
        v.setNull(0);
        v.combine(v);
        String str2 = v.getString();
        System.out.println(str2);
    }
    @Test
    public void test_BasicVoidVector_getDataCategory() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        BasicVoidVector v = new BasicVoidVector(6);
        v.setNull(0);
        v.getDataCategory();
        String str2 = v.getDataCategory().toString();
        System.out.println(str2);
        Assert.assertEquals("NOTHING",str2);

    }
    @Test
    public void test_BasicVoidVector_getDataType() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        BasicVoidVector v = new BasicVoidVector(6);
        v.setNull(0);
        v.getDataCategory();
        String str2 = v.getDataType().toString();
        System.out.println(str2);
        Assert.assertEquals("DT_VOID",str2);
    }
    @Test
    public void test_BasicVoidVector_getElementClass() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        BasicVoidVector v = new BasicVoidVector(6);
        v.setNull(0);
        v.getDataCategory();
        String str2 = v.getElementClass().toString();
        System.out.println(str2);
        Assert.assertEquals("class com.xxdb.data.Void",str2);
    }
    @Test
    public void test_BasicVoidVector_rows() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        BasicVoidVector v = new BasicVoidVector(6);
        v.setNull(0);
        System.out.println(v.rows());
        Assert.assertEquals(6,v.rows());
    }
    @Test
    public void test_BasicVoidVector_asof() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        BasicVoidVector v = new BasicVoidVector(6);
        v.setNull(0);
        Entity aa = (BasicInt)conn.run("1");
        try{
            v.asof((Scalar) aa);
        } catch (Exception e) {
            Assert.assertEquals("BasicVoidVector.asof not supported.",e.getMessage());
        }
    }
    @Test
    public void test_BasicVoidVector_getUnitLength() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        BasicVoidVector v = new BasicVoidVector(6);
        v.setNull(0);
        Assert.assertEquals(1,v.getUnitLength());
    }
    @Test
    public void test_BasicVoidVector_Append() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        BasicVoidVector v = new BasicVoidVector(6);
        Assert.assertEquals(6,v.rows());
        Entity aa = (BasicInt)conn.run("1");
        v.Append((Scalar) aa);
        System.out.println(v.rows());
        Assert.assertEquals(7,v.rows());
    }
    @Test
    public void test_BasicVoidVector_Append_1() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        BasicVoidVector v = new BasicVoidVector(6);
        Assert.assertEquals(6,v.rows());
        v.Append(v);
        System.out.println(v.rows());
        Assert.assertEquals(12,v.rows());
    }
    @Test
    public void test_BasicVoidVector_writeVectorToBuffer() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        BasicVoidVector v = new BasicVoidVector(6);
        Assert.assertEquals(6,v.rows());
        v.Append(v);
        System.out.println(v.rows());
        ByteBuffer vv = v.writeVectorToBuffer(ByteBuffer.allocate(8));
        System.out.println(Arrays.toString(vv.array()));
        Assert.assertEquals("[0, 0, 0, 0, 0, 0, 0, 0]",Arrays.toString(vv.array()));
    }

    @Test
    public void test_BasicVoidVector_run() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run("n = 1000;void1 = table(1..n as id);");
        BasicTable table = (BasicTable)conn.run("tmp = select *,NULL as value1,NULL as value2,NULL as value3,NULL as value4 from void1;tmp;");
        Assert.assertEquals(1000,table.rows());
        Assert.assertEquals("",((Void)((BasicVoidVector) table.getColumn("value1")).get(0)).getString());
        conn.run("t2 = table(100:0, `id`value1`value2`value3`value4, [INT,INT,DOUBLE,LONG,STRING]);t2.append!(tmp)");
        BasicTable table1 = (BasicTable)conn.run("select * from t2");
        Assert.assertEquals(1000,table1.rows());
    }
    @Test
    public void test_BasicVoidVector_upload() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run("n = 5;void1 = table(1..n as id);");
        BasicTable table = (BasicTable)conn.run("tmp = select *,NULL as value1,NULL as value2,NULL as value3,NULL as value4 from void1;tmp;");
        Assert.assertEquals(5,table.rows());
        Assert.assertEquals("",((Void)((BasicVoidVector) table.getColumn("value1")).get(0)).getString());
        BasicIntVector idv = (BasicIntVector) table.getColumn("id");
        BasicVoidVector value1v = (BasicVoidVector) table.getColumn("value1");
        BasicVoidVector value2v = (BasicVoidVector) table.getColumn("value2");
        BasicVoidVector value3v = (BasicVoidVector) table.getColumn("value3");
        BasicVoidVector value4v = (BasicVoidVector) table.getColumn("value4");
        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("id", idv);
        map.put("value1", value1v);
        map.put("value2", value2v);
        map.put("value3", value3v);
        map.put("value4", value4v);
        conn.upload(map);
        BasicVoidVector value1 = (BasicVoidVector) conn.run("value1");
        BasicVoidVector value2 = (BasicVoidVector) conn.run("value2");
        BasicVoidVector value3 = (BasicVoidVector) conn.run("value3");
        BasicVoidVector value4 = (BasicVoidVector) conn.run("value4");
        Assert.assertEquals("[,,,,]",value1.getString());
        Assert.assertEquals("[,,,,]",value2.getString());
        Assert.assertEquals("[,,,,]",value3.getString());
        Assert.assertEquals("[,,,,]",value4.getString());
    }

    @Test
    public void test_BasicVoidVector_upload_table() throws Exception {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run("n = 1000;void1 = table(1..n as id);");
        BasicTable table = (BasicTable)conn.run("tmp = select *,NULL as value1,NULL as value2,NULL as value3,NULL as value4 from void1;tmp;");
        Assert.assertEquals(1000,table.rows());
        Assert.assertEquals("",((Void)((BasicVoidVector) table.getColumn("value1")).get(0)).getString());
        Map<String, Entity> upObj = new HashMap<String, Entity>();
        upObj.put("table_upload_table", (Entity) table);
        conn.upload(upObj);
        BasicTable table1 = (BasicTable) conn.run("table_upload_table");
        assertEquals(1000, table1.rows());
        assertEquals(5, table1.columns());
        Assert.assertEquals("",((Void)((BasicVoidVector) table1.getColumn("value1")).get(0)).getString());
    }
    @Test
    public void test_void_tableInsert() throws IOException {
        conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run("n = 1000;void1 = table(1..n as id);");
        BasicTable table = (BasicTable)conn.run("tmp = select *,NULL as value1,NULL as value2,NULL as value3,NULL as value4 from void1;tmp;");
        Assert.assertEquals(1000,table.rows());
        List<Entity> args = Arrays.asList(table);
        conn.run("tb = table(100:0, `id`value1`value2`value3`value4, [INT,INT,DOUBLE,LONG,STRING]);");
        BasicInt re = (BasicInt) conn.run("tableInsert{tb}", args);
        assertEquals(1000, re.getInt());
        BasicTable table1 = (BasicTable) conn.run("select * from tb");
        assertEquals(1000, table1.rows());
        assertEquals(5, table1.columns());
        Assert.assertEquals("",(((BasicIntVector) table1.getColumn("value1")).get(0)).getString());
    }
//    @Test
//    public void test_void_mtw() throws Exception {
//        conn = new DBConnection();
//        conn.connect(HOST,PORT,"admin","123456");
//        conn.run("n = 1000;t1 = table(1..n as id);");
//        BasicTable table = (BasicTable)conn.run("tmp = select *,NULL as value1,NULL as value2,NULL as value3,NULL as value4 from t1;tmp;");
//        Assert.assertEquals(1000,table.rows());
//        List<Entity> args = Arrays.asList(table);
//        conn.run("share table(100:0, `id`value1`value2`value3`value4, [INT,INT,DOUBLE,LONG,STRING]) as tb");
//        MultithreadedTableWriter mtw = new MultithreadedTableWriter(HOST, PORT, "admin", "123456",
//                "", "tb", false, false, null, 1, 1,
//                1,"id");
//        try{
//            ErrorCodeInfo pErrorInfo = mtw.insert(args);
//        }
//        catch(RuntimeException ex)
//        {
//            System.out.println(ex.getMessage());
//        }
//        mtw.waitForThreadCompletion();
//        BasicTable table1 = (BasicTable) conn.run("select * from tb");
//        assertEquals(1000, table1.rows());
//        assertEquals(5, table1.columns());
//       Assert.assertEquals("",(((BasicIntVector) table1.getColumn("value1")).get(0)).getString());
//        conn.run("undef(`tb1)");
//    }
}

