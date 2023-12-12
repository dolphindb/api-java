package examples;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.util.*;

/**
 * show the basic usage and conversion between DolphinDB Entity and Java data types。
 */
public class DataTypeDemo {
    private static void scalarDemo(){
        BasicInt basicInt = new BasicInt(1);
        BasicDouble basicDouble = new BasicDouble(1.0);
        BasicDate basicDate = new BasicDate(LocalDate.of(2024, 1, 1));

        int a = basicInt.getInt();
        double b = basicDouble.getDouble();
        LocalDate c = basicDate.getDate();
    }

    private static void vectorDemo(){
        BasicIntVector intVector = new BasicIntVector(5);
        intVector.add(10);
        intVector.add(20);
        intVector.add(30);

        //get by index, start at 0
        int value = intVector.getInt(1);
        intVector.setInt(2, 40);
    }

    private static void matrixDemo(){
        BasicIntMatrix intMatrix = new BasicIntMatrix(3, 3);

        // set by index and value
        intMatrix.setInt(0, 0, 10);
        intMatrix.setInt(1, 1, 20);
        intMatrix.setInt(2, 2, 30);

        // get by index, start at [0, 0]
        int valueOfMatrix = intMatrix.getInt(1, 1);

        boolean isNull = intMatrix.isNull(0, 1);
        intMatrix.setNull(0, 1);
    }

    private static void setDemo(){
        BasicSet intSet = new BasicSet(Entity.DATA_TYPE.DT_INT);
        BasicInt intElement1 = new BasicInt(10);
        BasicInt intElement2 = new BasicInt(20);
        intSet.add(intElement1);
        intSet.add(intElement2);

        Vector keysVector = intSet.keys();
        int size = intSet.rows();
        boolean containsElement = intSet.contains(intElement1);
    }

    private static void dictDemo(){
        BasicDictionary dict = new BasicDictionary(Entity.DATA_TYPE.DT_STRING, Entity.DATA_TYPE.DT_INT);

        Scalar key1 = new BasicString("key1");
        Scalar value1 = new BasicInt(10);
        dict.put(key1, value1);

        Scalar key2 = new BasicString("key2");
        Scalar value2 = new BasicInt(20);
        dict.put(key2, value2);

        Entity retrievedValue = dict.get(key1);
        int sizeOfDict = dict.rows();
        java.util.Set<Entity> keysSet =  dict.keys();
        Collection<Entity> valuesCollection = dict.values();
        java.util.Set entriesSet = dict.entrySet();
    }

    private static void tableDemo(){

        List<String> columnNames = new ArrayList<>();
        columnNames.add("Name");
        columnNames.add("Age");
        List<Vector> columns = new ArrayList<>();
        columns.add(new BasicStringVector(Arrays.asList("Alice", "Bob", "Charlie")));
        columns.add(new BasicIntVector(Arrays.asList(25, 30, 28)));

        BasicTable table = new BasicTable(columnNames, columns);

        int rowCount = table.rows();
        int colCount = table.columns();

        // get Column by index or name
        Vector nameColumn = table.getColumn(0);
        Vector ageColumn = table.getColumn(1);
        Vector nameColumnByName = table.getColumn("Name");

        // get a brief string
        String tableString = table.getString();

        // get sub table
        Table subTableRange = table.getSubTable(0, 1);

        Vector newColumn = new BasicDoubleVector(Arrays.asList(5.5, 6.6, 7.7));
        table.addColumn("Height", newColumn);
    }

    private static void dataTypeTrans(){

        BasicInt basicInt = new BasicInt(1);
        int i = basicInt.getInt();
        BasicDouble basicDouble = new BasicDouble(1.0);
        double d = basicDouble.getDouble();
        BasicString basicString = new BasicString("Hello World!");
        String s = basicString.getString();
        BasicBoolean basicBoolean = new BasicBoolean(true);
        boolean b = basicBoolean.getBoolean();
        BasicByte basicByte = new BasicByte((byte) 1);
        byte by = basicByte.getByte();
        BasicShort basicShort = new BasicShort((short) 1);
        short sh = basicShort.getShort();
        BasicLong basicLong = new BasicLong(1L);
        long l = basicLong.getLong();
        BasicFloat basicFloat = new BasicFloat(1.0f);
        float f = basicFloat.getFloat();

        BasicUuid basicUuid = BasicUuid.random();
        UUID uuid = UUID.fromString(basicUuid.getString());

        BasicDecimal32 basicDecimal32 = new BasicDecimal32("34567.1", 2);
        BigDecimal decimal32 = new BigDecimal(basicDecimal32.getString());

        BasicDate basicDate = new BasicDate(LocalDate.now());
        LocalDate date = basicDate.getDate();

        BasicDateTime basicDateTime = new BasicDateTime(LocalDateTime.now());
        LocalDateTime dateTime = basicDateTime.getDateTime();

        BasicNanoTimestamp basicNanoTimestamp = new BasicNanoTimestamp(LocalDateTime.now());
        LocalDateTime nanoTimestamp = basicNanoTimestamp.getNanoTimestamp();

        BasicTimestamp basicTimestamp = new BasicTimestamp(LocalDateTime.now());
        LocalDateTime timestamp = basicTimestamp.getTimestamp();

        BasicDateHour basicDateHour = new BasicDateHour(LocalDateTime.now());
        LocalDateTime dateHour = basicDateHour.getDateHour();

        BasicMinute basicMinute = new BasicMinute(LocalTime.now());
        LocalTime minute = basicMinute.getMinute();

        BasicSecond basicSecond = new BasicSecond(LocalTime.now());
        LocalTime second = basicSecond.getSecond();

        BasicNanoTime basicNanoTime = new BasicNanoTime(LocalTime.now());
        LocalTime nanoTime = basicNanoTime.getNanoTime();

        BasicTime basicTime = new BasicTime(LocalTime.now());
        LocalTime time1 = basicTime.getTime();

        BasicMonth basicMonth = new BasicMonth(YearMonth.now());
        YearMonth month = basicMonth.getMonth();
    }



    public static void main(String[] args) throws Exception {
         scalarDemo();
         vectorDemo();
         matrixDemo();
         setDemo();
         dictDemo();
         tableDemo();
         dataTypeTrans();
    }

}
