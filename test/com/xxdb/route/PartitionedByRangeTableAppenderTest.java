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
public class PartitionedByRangeTableAppenderTest {
    static Random random = new Random();
    static final String[] symbols = new String[]{"A","AA","AABWS","AAC","AACC","AAI","AAME","AANB","AAON","AAP","AAPL",
            "AAPR","AAR","AATI","AAU","AAUKD","AAV","AAWW","AAY","AAZST","AB","ABA","ABAX","ABB","ABBC","ABBI",
            "ABC","ABCB","ABCO","ABCW","ABD","ABER","ABFS","ABG","ABI","ABIX","ABK","ABL","ABM","ABMC","ABMD",
            "ABN","ABNJ","ABNPRE","ABNPRF","ABNPRG","ABP","ABPI","ABR","ABT","ABTL","ABV","ABVA","ABVC","ABWPRA",
            "ABX","ABXA","ABY","ACA","ACAD","ACAP","ACAS","ACAT","ACBA","ACC","ACCL","ACE","ACEC","ACEL","ACEPRC",
            "ACET","ACF","ACFC","ACG","ACGL","ACGY","ACH","ACHN","ACI","ACIW","ACL","ACLI","ACLS","ACM","ACME",
            "ACMR","ACN","ACO","ACOR","ACP","ACPW","ACS","ACTG","ACTI","ACTL","ACTS","ACTU","ACU","ACUS","ACV",
            "ACW","ACXM","ACY","ADAM","ADAT","ADBE","ADBL","ADC","ADCT","ADEP","ADES","ADF","ADG","ADH","ADI",
            "ADK","ADKWS","ADL","ADLR","ADLS","ADM","ADP","ADPI","ADPT","ADRA","ADRD","ADRE","ADRU","ADS","ADSK",
            "ADST","ADSX","ADTN","ADVNA","ADVNB","ADVS","ADX","ADY","AE","AEA","AEB","AEC","AECPRB","AED","AEE",
            "AEG","AEH","AEHR","AEIS","AEL","AEM","AEMLW","AEN","AEO","AEP","AEPI","AER","AERO","AERT","AES",
            "AESPRC","AET","AETI","AEV","AEY","AEZ","AEZS","AF","AFAM","AFB","AFC","AFCE","AFE","AFF","AFFM",
            "AFFX","AFFY","AFG","AFL","AFN","AFO","AFOP","AFP","AFR","AFSI","AFT","AG","AGC","AGD","AGE","AGEN",
            "AGG","AGII","AGIX","AGL","AGM","AGMA","AGN","AGO","AGP","AGT","AGU","AGYS","AHCI","AHD","AHG","AHGP",
            "AHHPRA","AHII","AHL","AHLPR","AHLPRA","AHM","AHMPRA","AHMPRB","AHN","AHO","AHPI","AHR","AHRPRC","AHRPRD",
            "AHS","AHT","AHTPRA","AHTPRD","AHY","AIB","AID","AIG","ACSEF","AGB","AGIID","ACPPR"};
    private static List<Entity> generateRandomRow() {
        BasicString symbol = new BasicString(symbols[random.nextInt(symbols.length)]);
        BasicDate date = new BasicDate(LocalDate.now());
        BasicSecond time = new BasicSecond(LocalTime.now());
        BasicDouble price = new BasicDouble(random.nextDouble() * 10);
        BasicInt size = new BasicInt(random.nextInt(100));
        BasicInt corr = new BasicInt(random.nextInt(16));
        BasicInt g127 = new BasicInt(random.nextInt(128));
        BasicString cond = new BasicString("A");
        BasicByte ex = new BasicByte((byte)'B');
        return Arrays.asList(symbol, date, time, price, size, g127, corr, cond, ex);
    }

    private static List<Entity> generateRandomRows(int n) {
        BasicStringVector vsymbol = new BasicStringVector(n);
        for(int i = 0; i < n; ++i) {
            vsymbol.setString(i, symbols[random.nextInt(symbols.length)]);
        }
        BasicDateVector vdate = new BasicDateVector(n);
        for(int i = 0; i < n; ++i) {
            vdate.setDate(i, LocalDate.now());
        }
        BasicSecondVector vtime = new BasicSecondVector(n);
        for(int i = 0; i < n; ++i) {
            vtime.setSecond(i, LocalTime.now());
        }
        BasicDoubleVector vprice = new BasicDoubleVector(n);
        for(int i = 0; i < n; ++i) {
            vprice.setDouble(i, random.nextDouble() * 10);
        }
        BasicIntVector vsize = new BasicIntVector(n);
        for(int i = 0; i < n; ++i) {
            vsize.setInt(i, random.nextInt(100));
        }
        BasicIntVector vcorr = new BasicIntVector(n);
        for(int i = 0; i < n; ++i) {
            vcorr.setInt(i, random.nextInt(16));
        }
        BasicIntVector vg127 = new BasicIntVector(n);
        for(int i = 0; i < n; ++i) {
            vg127.setInt(i, random.nextInt(128));
        }
        BasicStringVector vcond = new BasicStringVector(n);
        for(int i = 0; i < n; ++i) {
            vcond.setString(i, "A");
        }
        BasicByteVector vex = new BasicByteVector(n);
        for(int i = 0; i < n; ++i) {
            vex.setByte(i, (byte)'B');
        }
        return Arrays.asList(vsymbol, vdate, vtime, vprice, vsize, vg127, vcorr, vcond, vex);
    }
    public static int INSERTIONS = 20000000;
    public static int BATCH_SIZE = 100;

    public static void main(String[] args) {
        String host = "192.168.1.25";
        String tableName = "Trades";
        int port = 8847;
        if (args.length >= 1) {
            tableName = args[0];
        }
        if (args.length >= 2) {
            host = args[1];
        }
        if (args.length >= 3) {
            port = Integer.valueOf(args[2]);
        }
        PartitionedTableAppender appender = null;
        try {
            appender = new PartitionedTableAppender(tableName, host, port);
            int affected = 0;
            long start = System.currentTimeMillis();
            for(int i = 0; i < INSERTIONS; i += BATCH_SIZE) {
                affected += appender.append(generateRandomRows(BATCH_SIZE));
            }

            long end = System.currentTimeMillis();
            System.out.println("inserting " + affected + " rows took " + (end - start) + "ms, throughput: " + affected / ((end - start) / 1000.0) + " rows/s");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (appender != null)
                appender.shutdownThreadPool();
        }
    }
}
