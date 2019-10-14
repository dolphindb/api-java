package com.xxdb;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;

public class FileLog {


    public static void Log(String s){
        File dir = new File("");

        File f = new File(dir.getAbsolutePath() + "\\JavaApi.log");
        FileWriter fw = null;
        try {
            fw = new FileWriter(f , true);
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        PrintWriter pw = new PrintWriter(fw);
        pw.print(new Date().toString());
        pw.print(": ");
        pw.println();
        pw.print(s);
        pw.println();

        pw.flush();
        try {
            fw.flush();
            pw.close();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
