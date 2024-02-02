package com.xxdb.io;

import com.xxdb.io.BigEndianDataOutputStream;
import com.xxdb.io.ExtendedDataOutput;
import com.xxdb.io.Long2;
import com.xxdb.streaming.client.PollingClient;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketException;
import java.util.ResourceBundle;

import static org.junit.Assert.*;


public class BigEndianDataOutputStreamTest {
    static ResourceBundle bundle = ResourceBundle.getBundle("com/xxdb/setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));

    @Test
    public void test_BigEndianDataOutputStream_Basic() throws IOException {
        ExtendedDataOutput output = new BigEndianDataOutputStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                System.out.println(b);
            }
        });
        short[] shortv = {1, 2, 3, 4, 5};
        int[] intv = {1, 2, 3, 4, 5};
        long[] longv = {1, 2, 3, 4, 5};
        Long2[] long2v = new Long2[] {new Long2(1,1), new Long2(1,1)};
        output.writeShortArray(shortv,0,2);
        output.writeIntArray(intv,0,2);
        output.writeLongArray(longv,0,2);
        output.writeLong2Array(long2v,0,2);
        output.writeBoolean(true);
        output.writeBoolean(false);
        output.writeChar(1);
        output.writeChars("1");
        output.writeStringArray(new String[]{"12A","HELLO"});
        output.writeStringArray(new String[]{"12A","HELLO"},1,1);
    }
}
