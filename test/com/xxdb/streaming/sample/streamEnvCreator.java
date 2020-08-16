package com.xxdb.streaming.sample;

public class streamEnvCreator {
    String initScript = "def createStreamEnv(){\n" +
            "\t\tst1 = streamTable(10000:0,`symbol`date`time`price`vol,[SYMBOL,DATE,TIME,DOUBLE,INT])\n" +
            "\t\tenableTableShareAndPersistence(st1, `st1, true, true)\n" +
            "\t\tst2 = streamTable(10000:0,`symbol`date`time`price`vol,[SYMBOL,DATE,TIME,DOUBLE,INT])\n" +
            "\t\tenableTableShareAndPersistence(st2, `st2, true, false)\n" +
            "}\n" +
            "def writeData(n){\n" +
            "\t\tsyms = lpad(string(1..1000),6,`0)\n" +
            "\t\tfor(i in 1..n){\n" +
            "\t\t\tdata = table(rand(syms,1) as symbol, date(now()) as date, time(now()) as time, norm(1,0.5,1) as price, rand(1..100,1) as vol)\n" +
            "\t\t\tobjByName(`st1).append!(data)\n" +
            "\t\t\tobjByName(`st2).append!(data)\n" +
            "\t\t\tsleep(rand(1..100,1)[0])\n" +
            "\t\t}\n" +
            "}";
}
