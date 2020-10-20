package com.xxdb;

import com.xxdb.data.BasicTable;
import com.xxdb.data.EntityBlockReader;
import com.xxdb.io.ProgressListener;
import org.junit.Assert;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class FetchSizeThread implements Runnable {
    DBConnection conn = new DBConnection();
    public static String HOST;
    public static Integer PORT;

    @Override
    public void run() {
        try {
            conn.connect(HOST, PORT, "admin", "123456");
            EntityBlockReader v = (EntityBlockReader) conn.run("table(1..500000 as id)", (ProgressListener) null, 4, 4, 10000);
            BasicTable data = (BasicTable) v.read();
            while (v.hasNext()) {
                BasicTable t = (BasicTable) v.read();
                data = data.combine(t);
            }
            Assert.assertEquals(500000, data.rows());
            System.out.println("done");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            conn.close();
        }
    }

    public static void main(String[] args) {
        try {
            Properties props = new Properties();
            FileInputStream in = new FileInputStream("test/com/xxdb/setup/settings.properties");
            props.load(in);
            PORT = Integer.parseInt(props.getProperty("PORT"));
            HOST = props.getProperty("HOST");
            for (int i = 0; i < 50; i++) {
                new Thread(new FetchSizeThread()).start();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
