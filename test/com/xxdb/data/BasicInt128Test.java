package com.xxdb.data;

import com.xxdb.DBConnection;
import com.xxdb.io.BigEndianDataInputStream;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.Long2;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ResourceBundle;

import static org.junit.Assert.*;

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
                int expected = getServerHash(((Scalar)v.get(i)),b);
                Assert.assertEquals(expected, v.hashBucket(i, b));
                Assert.assertEquals(expected, ((Scalar)v.get(i)).hashBucket(b));
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

    @Test
    public void test_BasicInt128(){
        BasicInt128 bi128 = new BasicInt128(75L,10L);
        assertEquals(10L,bi128.getLong2().low);
        assertEquals(75L,bi128.getLong2().high);
        assertFalse(bi128.equals(new BasicDuration(Entity.DURATION.MONTH,Integer.MAX_VALUE)));
    }

    @Test(expected = NumberFormatException.class)
    public void test_BasicInt128_formString_false(){
        BasicInt128.fromString("130d6d5a0536c99ac7f9a0");
    }

    @Test
    public void test_BasicInt128Vector_list(){
        List<Long2> list = new ArrayList<>();
        list.add(new Long2(980L,5L));
        list.add(null);
        list.add(new Long2(2022L,0L));
        BasicInt128Vector bi128v = new BasicInt128Vector(list);
        assertTrue(bi128v.isNull(1));
        bi128v.setInt128(1,1566L,220L);
        assertEquals("[000000000000061e00000000000000dc" +
                ",00000000000007e60000000000000000," +
                "00000000000003d40000000000000005]",bi128v.getSubVector(new int[]{1,2,0}).getString());
        assertEquals(Entity.DATA_CATEGORY.BINARY,bi128v.getDataCategory());
        assertEquals(BasicInt128.class,bi128v.getElementClass());
    }

    @Test
    public void test_BasicInt128Vector_array() throws Exception {
        Long2[] arr = new Long2[]{new Long2(888L,800L),new Long2(220L,25L),null,new Long2(9000L,650L)};
        BasicInt128Vector bi128v = new BasicInt128Vector(arr,false);
        assertEquals("00000000000003780000000000000320",bi128v.get(0).getString());
        assertEquals("",bi128v.get(2).getString());
    }

    @Test
    public void test_BasicInt128Vector_df_in() throws IOException {
        ExtendedDataInput in = new BigEndianDataInputStream(new InputStream() {
            @Override
            public int read() throws IOException {
                return 2;
            }
        });
        BasicInt128Vector bi128v = new BasicInt128Vector(Entity.DATA_FORM.DF_VECTOR,in);
        assertEquals("02020202020202020202020202020202",bi128v.get(1).getString());
    }

    @Test(expected = RuntimeException.class)
    public void test_BasicInt128Vector_asof(){
        Long2[] arr = new Long2[]{new Long2(888L,800L),new Long2(220L,25L),null,new Long2(9000L,650L)};
        BasicInt128Vector bi128v = new BasicInt128Vector(arr,false);
        bi128v.asof(new BasicInt128(55L,11L));
    }

    @Test
    public void test_BasicInt128Vector_wvtb_littleEndian() throws IOException {
        Long2[] arr = new Long2[]{new Long2(888L,800L),new Long2(220L,25L),null,new Long2(9000L,650L)};
        BasicInt128Vector bi128v = new BasicInt128Vector(arr,false);
        ByteBuffer bb1 = ByteBuffer.allocate(64);
        bb1.order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer bb2 = bi128v.writeVectorToBuffer(bb1);
        assertEquals("[32, 3, 0, 0, 0, 0, 0, 0, " +
                "120, 3, 0, 0, 0, 0, 0, 0, 25, 0, 0, 0, " +
                "0, 0, 0, 0, -36, 0, 0, 0, 0, 0, 0, 0, " +
                "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, " +
                "0, 0, 0, 0, -118, 2, 0, 0, 0, 0, 0, " +
                "0, 40, 35, 0, 0, 0, 0, 0, 0]",Arrays.toString(bb2.array()));
    }

    @Test
    public void test_BasicInt128Vector_wvtb_BigEndian() throws IOException {
        Long2[] arr = new Long2[]{new Long2(888L,800L),new Long2(220L,25L),null,new Long2(9000L,650L)};
        BasicInt128Vector bi128v = new BasicInt128Vector(arr,false);
        ByteBuffer bb1 = ByteBuffer.allocate(64);
        bb1.order(ByteOrder.BIG_ENDIAN);
        ByteBuffer bb2 = bi128v.writeVectorToBuffer(bb1);
        assertEquals("[0, 0, 0, 0, 0, 0, 3, 120, 0, " +
                "0, 0, 0, 0, 0, 3, 32, 0, 0, 0, 0, 0, 0, 0, " +
                "-36, 0, 0, 0, 0, 0, 0, 0, 25, 0, 0, 0, 0, " +
                "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, " +
                "0, 0, 0, 0, 35, 40, 0, 0, 0, 0, 0, 0, 2, " +
                "-118]",Arrays.toString(bb2.array()));
    }

    @Test
    public void test_BasicInt128Vector_serialize_LittleEndian() throws IOException {
        Long2[] arr = new Long2[]{new Long2(888L,800L),new Long2(220L,25L),null,new Long2(9000L,650L)};
        BasicInt128Vector bi128v = new BasicInt128Vector(arr,false);
        AbstractVector.NumElementAndPartial numElementAndPartial = new AbstractVector.NumElementAndPartial(4,1);
        ByteBuffer bb = ByteBuffer.allocate(48);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(48,bi128v.serialize(0,0,3,numElementAndPartial,bb));
        assertEquals("[32, 3, 0, 0, 0, 0, 0, 0, 120, 3, 0, 0," +
                " 0, 0, 0, 0, 25, 0, 0, 0, 0, 0, 0, 0, -36, 0, 0, 0, " +
                "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, " +
                "0, 0, 0]",Arrays.toString(bb.array()));
    }

    @Test
    public void test_BasicInt128Vector_serialize_BigEndian() throws IOException {
        Long2[] arr = new Long2[]{new Long2(888L,800L),new Long2(220L,25L),null,new Long2(9000L,650L)};
        BasicInt128Vector bi128v = new BasicInt128Vector(arr,false);
        AbstractVector.NumElementAndPartial numElementAndPartial = new AbstractVector.NumElementAndPartial(4,1);
        ByteBuffer bb = ByteBuffer.allocate(48);
        bb.order(ByteOrder.BIG_ENDIAN);
        assertEquals(48,bi128v.serialize(0,0,3,numElementAndPartial,bb));
        assertEquals("[0, 0, 0, 0, 0, 0, 3, 120, 0, 0, 0, 0, 0, 0, 3, 32, " +
                "0, 0, 0, 0, 0, 0, 0, -36, 0, 0, 0, 0, 0, 0, 0, 25, 0, 0, 0, 0, " +
                "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]",Arrays.toString(bb.array()));
    }

    @Test
    public void test_BasicInt128Vector_deserialize() throws IOException {
        Long2[] arr = new Long2[]{new Long2(888L,800L),new Long2(220L,25L),null,new Long2(9000L,650L)};
        BasicInt128Vector bi128v = new BasicInt128Vector(arr,false);
        String str = bi128v.getString();
        System.out.println(str);
        ExtendedDataInput in = new BigEndianDataInputStream(new InputStream() {
            @Override
            public int read() throws IOException {
                return 6;
            }
        });
        bi128v.deserialize(1,3,in);
        String str2 = bi128v.getString();
        System.out.println(str2);
        assertNotEquals(str,str2);
    }

    @Test
    public void test_BasicInt128Vector_Append() throws Exception {
        BasicInt128Vector bi128v = new BasicInt128Vector(new Long2[]{new Long2(17L,6L),new Long2(29L,2L)});
        int size = bi128v.size;
        int capacity = bi128v.capaticy;
        bi128v.Append(new BasicInt128(35L,18L));
        assertEquals(size+1,bi128v.size);
        assertEquals(capacity*2,bi128v.capaticy);
        bi128v.Append(new BasicInt128Vector(new Long2[]{new Long2(3L,0L),new Long2(59L,17L)}));
        assertEquals(size+3,bi128v.size);
        assertEquals(capacity*2+2,bi128v.capaticy);
    }
}
