package com.xxdb.compression;

import com.xxdb.DBConnection;
import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicIntVector;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Entity;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LZ4Test {

    private DBConnection conn;
    public static String HOST = "127.0.0.1";
    public static Integer PORT = 8848;

    @Before
    public void setUp() {
        conn = new DBConnection(false, false, true);
        try {
            if (!conn.connect(HOST, PORT, "admin", "123456")) {
                throw new IOException("Failed to connect to 2xdb server");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testString() {
        BasicTable obj = null;
        try {
            obj = (BasicTable) conn.run("table(second(1..10240) as id, take(`hi`hello`hola, 10240) as value)");
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < obj.rows(); i++) {
            System.out.println(obj.getRowJson(i));
        }
    }

    @Test
    public void testLZ4(){
        LZ4Factory factory = LZ4Factory.fastestInstance();
        LZ4Compressor compressor = factory.fastCompressor();
        ByteBuffer buffer = ByteBuffer.allocate(100000);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        for(int i = 1; i <= 10000; ++i){
            buffer.putInt(i);
        }
        byte[] ret = compressor.compress(buffer.array(), 0, 4*10240);
        int a= 1;
    }

    @Test
    public void testcompressTable() throws IOException {
        BasicTable obj = null;
        try {
            obj = (BasicTable) conn.run("table(1..10000 as id)");
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < obj.rows(); i++) {
            System.out.println(obj.getRowJson(i));
        }
        conn.run("t = table(100000:0,[`id],[INT])" +
                "share t as st");
        List<String> colNames = new ArrayList<>();
        colNames.add("id");
        List<Entity> args = Arrays.asList(obj);
        BasicInt count = (BasicInt) conn.run("tableInsert{st}", args);
        BasicTable newT = (BasicTable) conn.run("select * from st");
    }

    @Test
    public void testCompressVector() throws IOException {
        conn.run("t = array(INT, 0)");
        List<Entity> args = new ArrayList<>();
        BasicIntVector obj = (BasicIntVector)conn.run("1..10000");
        args.add(obj);
        BasicInt count = (BasicInt) conn.run("append!{t}", args);
        BasicTable newT = (BasicTable) conn.run("select * from st");
    }


    @After
    public void tearDown() throws Exception {
        conn.close();
    }

}
