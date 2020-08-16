package com.xxdb.route;

import com.xxdb.data.*;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Created by root on 8/2/17.
 */
public class TableAppenderTest {
    static Random random = new Random();
    private static List<Entity> generateRandomRow() {
        BasicString symbol = new BasicString(String.valueOf((char)(random.nextInt(26) + 'A')));
        BasicDate date = new BasicDate(LocalDate.now());
        BasicSecond time = new BasicSecond(LocalTime.now());
        BasicDouble price = new BasicDouble(random.nextDouble() * 10);
        BasicInt size = new BasicInt(random.nextInt(100));
        BasicInt corr = new BasicInt(random.nextInt(16));
        BasicInt g127 = new BasicInt(random.nextInt(128));
        BasicString cond = new BasicString("china");
        BasicByte ex = new BasicByte((byte)'B');
        return Arrays.asList(symbol, date, time, price, size, g127, corr, cond, ex);
    }

    public static void main(String[] args) {
        try {
            TableAppender appender = new TableAppender("t3", "192.168.1.25", 8848);
            System.out.println(appender.append(generateRandomRow()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
