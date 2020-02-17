import com.xxdb.data.*;
import com.xxdb.DBConnection;
import com.xxdb.data.Vector;

import java.io.IOException;
import java.time.*;
import java.util.*;

public class table_writer {
    /**
     * This method bulk loads a table to DolphinDB partitioned historical database through Java API.
     * The whole process consists of three steps:
     * (1) Prepare a DolphinDB table in Java.
     * (2) Upload the table to DolphinDB Server
     * (3) Call savePartition function on server to a historical database
     */

    private DBConnection conn;
    private int caseCount;
    private int failedCount;
    public void testBulkLoad() throws IOException {
        //prepare a table with five columns
        List<String> colNames = new ArrayList<String>();
        colNames.add("sym");
        colNames.add("date");
        colNames.add("time");
        colNames.add("price");
        colNames.add("qty");

        List<Vector> cols = new ArrayList<Vector>();
        int n = 5000000;

        String[] syms = new String[]{"AAPL","AMZN","AB"};
        String[] vsym = new String[n];
        int[] indices = generateRandomIntegers(syms.length, n);
        for(int i=0; i<n; ++i)
            vsym[i] = syms[indices[i]];
        cols.add(new BasicStringVector(vsym));

        BasicDate date = new BasicDate(LocalDate.of(2017,8, 7));
        int[] dates = new int[]{0,1,2};
        BasicDateVector vdate = new BasicDateVector(n);
        indices = generateRandomIntegers(dates.length, n);
        for(int i=0; i<n; ++i)
            vdate.setInt(i, date.getInt() + dates[indices[i]]);
        cols.add(vdate);

        BasicTime time = new BasicTime(LocalTime.of(9,30, 0));
        BasicTimeVector vtime = new BasicTimeVector(n);
        indices = generateRandomIntegers(21600000, n);
        for(int i=0; i<n; ++i)
            vtime.setInt(i, time.getInt() + indices[i]);
        cols.add(vtime);


        double[] prices = new double[]{7.8, 7.6, 7.1, 7.5, 7.4};
        double[] vprice = new double[n];
        indices = generateRandomIntegers(prices.length, n);
        for(int i=0; i<n; ++i)
            vprice[i] = prices[indices[i]];
        cols.add(new BasicDoubleVector(vprice));

        int[] qtys = new int[]{5,4,3,2,1};
        int[] vqty = new int[n];
        indices = generateRandomIntegers(qtys.length, n);
        for(int i=0; i<n; ++i)
            vqty[i] = qtys[indices[i]];
        cols.add(new BasicIntVector(vqty));;

        BasicTable t1 = new BasicTable(colNames, cols);

        //upload the table to DolphinDB server
        Map<String, Entity> vars = new HashMap<String, Entity>();
        vars.put("t1", t1);
        LocalDateTime start = LocalDateTime.now();
        //conn.upload(vars);
        Duration elapsed = Duration.between(start, LocalDateTime.now());
        System.out.println("Table upload time: " + elapsed.getSeconds() + " s " + (elapsed.getNano()/1000000));

        /**
         * run the script on server to save data to historical database
         * (1) create or open the database which has value-based partition on date.
         * (2) convert the column sym from STRING type to SYMBOL type. The SYMBOL type has much better query performance than STRING type.
         * (3) call savePartition function to dump data to historical database on disk.
         */
        String script = "tickdb = database('c:/DolphinDB/db_testing/TickDB_A', VALUE, 2000.01.01..2024.12.31) \n" +
                "t1[`sym] = symbol(t1.sym) \n" +
                "tb = tickdb.createPartitionedTable(t1,`Trades,`date) \n" +
                "tb.append!(t1)";

        start = LocalDateTime.now();
        conn.run(script);
        elapsed = Duration.between(start, LocalDateTime.now());
        System.out.println("Table save time: " + elapsed.getSeconds() + " s " + (elapsed.getNano()/1000000));
    }

    private int[] generateRandomIntegers(int uplimit, int count){
        Random randomGenerator = new Random();
        int[] indices = new int[count];
        for(int i=0; i<count; ++i)
            indices[i] = randomGenerator.nextInt(uplimit);
        return indices;
    }

    private	BasicTable createBasicTable(){
        List<String> colNames = new ArrayList<String>();
        colNames.add("cbool");
        colNames.add("cchar");
        colNames.add("cshort");
        colNames.add("cint");
        colNames.add("clong");
        colNames.add("cdate");
        colNames.add("cmonth");
        colNames.add("ctime");
        colNames.add("cminute");
        colNames.add("csecond");
        colNames.add("cdatetime");
        colNames.add("ctimestamp");
        colNames.add("cnanotime");
        colNames.add("cnanotimestamp");
        colNames.add("cfloat");
        colNames.add("cdouble");
        colNames.add("csymbol");
        colNames.add("cstring");
        List<Vector> cols = new ArrayList<Vector>(){};


        //boolean
        byte[] vbool = new byte[]{1,0};
        BasicBooleanVector bbv = new BasicBooleanVector(vbool);
        cols.add(bbv);
        //char
        byte[] vchar = new byte[]{(byte)'c',(byte)'a'};
        BasicByteVector bcv = new BasicByteVector(vchar);
        cols.add(bcv);
        //cshort
        short[] vshort = new short[]{32767,29};
        BasicShortVector bshv = new BasicShortVector(vshort);
        cols.add(bshv);
        //cint
        int[] vint = new int[]{2147483647,483647};
        BasicIntVector bintv = new BasicIntVector(vint);
        cols.add(bintv);
        //clong
        long[] vlong = new long[]{2147483647,483647};
        BasicLongVector blongv = new BasicLongVector(vlong);
        cols.add(blongv);
        //cdate
        int[] vdate = new int[]{Utils.countDays(LocalDate.of(2018,2,14)),Utils.countDays(LocalDate.of(2018,8,15))};
        BasicDateVector bdatev = new BasicDateVector(vdate);
        cols.add(bdatev);
        //cmonth
        int[] vmonth = new int[]{Utils.countMonths(YearMonth.of(2018,2)),Utils.countMonths(YearMonth.of(2018,8))};
        BasicMonthVector bmonthv = new BasicMonthVector(vmonth);
        cols.add(bmonthv);
        //ctime
        int[] vtime = new int[]{Utils.countMilliseconds(16,46,05,123),Utils.countMilliseconds(18,32,05,321)};
        BasicTimeVector btimev = new BasicTimeVector(vtime);
        cols.add(btimev);
        //cminute
        int[] vminute = new int[]{Utils.countMinutes(LocalTime.of(16,30)),Utils.countMinutes(LocalTime.of(9,30))};
        BasicMinuteVector bminutev = new BasicMinuteVector(vminute);
        cols.add(bminutev);
        //csecond
        int[] vsecond = new int[]{Utils.countSeconds(LocalTime.of(9,30,30)),Utils.countSeconds(LocalTime.of(16,30,50))};
        BasicSecondVector bsecondv = new BasicSecondVector(vsecond);
        cols.add(bsecondv);
        //cdatetime
        int[] vdatetime = new int[]{Utils.countSeconds(LocalDateTime.of(2018,9,8,9,30,01)),Utils.countSeconds(LocalDateTime.of(2018,11,8,16,30,01))};
        BasicDateTimeVector bdatetimev = new BasicDateTimeVector(vdatetime);
        cols.add(bdatetimev);
        //ctimestamp
        long[] vtimestamp = new long[]{Utils.countMilliseconds(2018,11,12,9,30,01,123),Utils.countMilliseconds(2018,11,12,16,30,01,123)};
        BasicTimestampVector btimestampv = new BasicTimestampVector(vtimestamp);
        cols.add(btimestampv);
        //cnanotime
        long[] vnanotime = new long[]{Utils.countNanoseconds(LocalTime.of(9,30,05,123456789)),Utils.countNanoseconds(LocalTime.of(16,30,05,987654321))};
        BasicNanoTimeVector bnanotimev = new BasicNanoTimeVector(vnanotime);
        cols.add(bnanotimev);
        //cnanotimestamp
        long[] vnanotimestamp = new long[]{Utils.countNanoseconds(LocalDateTime.of(2018,11,12,9,30,05,123456789)),Utils.countNanoseconds(LocalDateTime.of(2018,11,13,16,30,05,987654321))};
        BasicNanoTimestampVector bnanotimestampv = new BasicNanoTimestampVector(vnanotimestamp);
        cols.add(bnanotimestampv);
        //cfloat
        float[] vfloat = new float[]{2147.483647f,483.647f};
        BasicFloatVector bfloatv = new BasicFloatVector(vfloat);
        cols.add(bfloatv);
        //cdouble
        double[] vdouble = new double[]{214.7483647,48.3647};
        BasicDoubleVector bdoublev = new BasicDoubleVector(vdouble);
        cols.add(bdoublev);
        //csymbol
        String[] vsymbol = new String[]{"GOOG","MS"};
        BasicStringVector bsymbolv = new BasicStringVector(vsymbol);
        cols.add(bsymbolv);
        //cstring
        String[] vstring = new String[]{"","test string"};
        BasicStringVector bstringv = new BasicStringVector(vstring);
        cols.add(bstringv);
        BasicTable t1 = new BasicTable(colNames, cols);
        return t1;
    }
    public void test_save_memoryTable() throws IOException{
        System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
        caseCount++;
        BasicTable table1 = createBasicTable();
        conn.run("t = table(10000:0,`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring,[BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING])\n");
        conn.run("share t as memoryTable");
        conn.run("def saveData(data){ memoryTable.append!(data)}");
        List<Entity> args = new ArrayList<Entity>(1);
        args.add(table1);
        conn.run("saveData", args);
        BasicTable dt = (BasicTable)conn.run("memoryTable");
        if(dt.rows()!=2){
            failedCount++;
            System.out.println("failed");
        }

    }

    public void test_save_dfsTable() throws IOException{
        System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
        caseCount++;
        BasicTable table1 = createBasicTable();
        conn.login("admin","123456",false);
        conn.run("t = table(10000:0,`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring,[BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING])\n");
        conn.run("if(existsDatabase('dfs://testDatabase')){dropDatabase('dfs://testDatabase')}");
        conn.run("db = database('dfs://testDatabase',RANGE,2018.01.01..2018.12.31)");
        conn.run("db.createPartitionedTable(t,'tb1','cdate')");
        conn.run("def saveData(data){ loadTable('dfs://testDatabase','tb1').append!(data)}");
        List<Entity> args = new ArrayList<Entity>(1);
        args.add(table1);
        conn.run("saveData", args);
        BasicTable dt = (BasicTable)conn.run("select * from loadTable('dfs://testDatabase','tb1')");
        if(dt.rows()!=2){
            failedCount++;
            System.out.println("failed");
        }
    }

    public void test_save_localTable(List<Byte> boolArray, List<Integer> intArray, double[] dblArray, List<Integer> dateArray, List<String> strArray) throws IOException{
        System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
        caseCount++;
        List<String> colNames =  Arrays.asList("cbool","cint","cdouble","cdate","cstring");
        List<Vector> cols = Arrays.asList(new BasicBooleanVector(boolArray),new BasicIntVector(intArray),new BasicDoubleVector(dblArray),new BasicDateVector(dateArray),new BasicStringVector(strArray));
        BasicTable table1 = new BasicTable(colNames,cols);
        String dbpath = "/home/user1/testDatabase";
        conn.run("t = table(10000:0,`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring,[BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING])\n");
        conn.run(String.format("if(existsDatabase('{0}')){dropDatabase('{0}')}",dbpath));
        conn.run(String.format("db = database('{0}',VALUE,'MS' 'GOOG' 'FB')",dbpath));
        conn.run("db.createPartitionedTable(t,'tb1','csymbol')");
        conn.run(String.format("def saveData(data){ loadTable('{0}','tb1').append!(data)}",dbpath));
        List<Entity> args = new ArrayList<Entity>(1);
        args.add(table1);
        conn.run("saveData", args);
    }

    public void test_loop_basicTable(BasicTable table1) throws Exception{
        System.out.println("Running "+ Thread.currentThread().getStackTrace()[1].getMethodName());
        caseCount++;
        BasicStringVector stringv = (BasicStringVector) table1.getColumn("cstring");
        BasicIntVector intv = (BasicIntVector) table1.getColumn("cint");
        BasicTimestampVector timestampv = (BasicTimestampVector) table1.getColumn("ctimestamp");
        BasicDoubleVector doublev = (BasicDoubleVector) table1.getColumn("cdouble");
        for(int ri=0;ri<table1.rows();ri++){
            System.out.println(stringv.getString(ri));
            System.out.println(intv.getInt(ri));
            LocalDateTime timestamp = timestampv.getTimestamp(ri);
            System.out.println(timestamp);
            System.out.println(doublev.getDouble(ri));
        }
    }


    public BasicTable buildTable(List<String> colNames , List<String> colTypes, int msgSize) throws Exception{
        List<Vector> colData = new ArrayList<Vector>();
        for (String colType:colTypes) {
            switch (colType){
                case "SYMBOL":
                    colData.add(new BasicStringVector(msgSize));
                    break;
                case "DATE":
                    colData.add(new BasicDateVector(msgSize));
                    break;
                case "FLOAT":
                    colData.add(new BasicFloatVector(msgSize));
                    break;
                case "INT":
                    colData.add(new BasicIntVector(msgSize));
                    break;
                case "LONG":
                    colData.add(new BasicLongVector(msgSize));
                    break;
                case "STRING":
                    colData.add(new BasicStringVector(msgSize));
                    break;
                case "TIME":
                    colData.add(new BasicTimeVector(msgSize));
                    break;
                case "TIMESTAMP":
                    colData.add(new BasicTimestampVector(msgSize));
                    break;
            }
        }
        return new BasicTable(colNames,colData);
    }


    private void bulk_load() throws Exception{
        List<String> colNames =  Arrays.asList("cbool","cint","cdouble","cdate","cstring");
        List<String> colTypes = Arrays.asList("BOOL","INT","DOUBLE","DATE","SYMBOL");
        //msgSize
        int msgSize = 100;
        BasicTable table1 = buildTable(colNames,colTypes, msgSize);
        for(int i=0;i<msgSize;i++){ //行数
            for(int j=0;j<colNames.size();j++){ //列数，从key中取
                String key = "colName1";
                String val = "1234";//假设value是string
                setTableValue(table1,i,key, val);
            }
        }
        List<Entity> args = new ArrayList<Entity>(1);
        args.add(table1);
        conn.run("tableInsert{market_data}", args);
    }

    private void setTableValue(BasicTable bt, int rowIndex, String key, String value) throws Exception {
        Scalar v = null;
        switch (bt.getColumn(key).getDataType()){
            case DT_STRING:
                v = new BasicString(value);
                break;
            case DT_DOUBLE:
                v = new BasicDouble(Double.parseDouble(value));
                break;
            case DT_FLOAT:
                v = new BasicFloat(Float.parseFloat(value));
                break;
            case DT_LONG:
                v = new BasicLong(Long.parseLong(value));
                break;
            case DT_INT:
                v = new BasicInt(Integer.parseInt(value));
                break;
            case DT_DATE:
                if(value.equals("")){
                    v = new BasicDate(LocalDate.now());
                    v.setNull();
                }else {
                    v = new BasicDate(LocalDate.parse(value));
                }
                //v = new BasicDate(Utils.parseDate(Integer.parseInt(value)));
                break;
            case DT_TIMESTAMP:
                if(value.equals("")) {
                    v = new BasicTimestamp(LocalDateTime.now());
                    v.setNull();
                }else {
                    v = new BasicTimestamp(LocalDateTime.parse(value));
                }
                //v = new BasicTimestamp(Utils.parseTimestamp(Long.parseLong(value)));
                break;
            case DT_TIME:
                if(value.equals("")) {
                    v = new BasicTime(LocalTime.now());
                    v.setNull();
                }else {
                    v = new BasicTime(LocalTime.parse(value));
                }
                //v = new BasicTime(Utils.parseTime(Integer.parseInt(value)));
                break;
        }
        bt.getColumn(key).set(rowIndex,v);
    }
}