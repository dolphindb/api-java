### 1. Java API Introduction

DolphinDB Java API requires Java 1.8 or higher environment.

Java API adopts interface-oriented programming. It uses the interface "Entity" to represent all data types returned from DolphinDB. Java API provides 7 types of extended interfaces: scalar, vector, matrix, set, dictionary, table and chart based on the "Entity" interface and DolphinDB data forms. They are included in the package of com.xxdb.data.

Extended interface classes | Naming rules | Examples
---|---|---
scalar|Basic\<DataType\>|BasicInt, BasicDouble, BasicDate, etc.
vector, matrix|Basic\<DataType\>\<DataForm\>|BasicIntVector, BasicDoubleMatrix, BasicAnyVector, etc.
set, dictionary, table|Basic\<DataForm\>|BasicSet, BasicDictionary, BasicTable.
chart||BasicChart

"Basic" indicates the basic implementation of a data form interface, \<DataType\> indicates a DolphinDB data type, and \<DataForm\> indicates a DolphinDB data form. For more information about interface and class, please refer to [Java API Manual](https://www.dolphindb.com/javaapi/).

The most important object provided by DolphinDB Java API is DBConnection. It allows Java applications to execute script and functions on DolphinDB servers and transfer data between Java applications and DolphinDB servers in both directions. The DBConnection class provides the following main methods:

| Method Name | Details |
|:------------- |:-------------|
|connect(host, port, [username, password])|Connect the session to DolphinDB server|
|login(username,password,enableEncryption)|Log in to DolphinDB server|
|run(script)|Run script on DolphinDB server|
|run(functionName,args)|Call a function on DolphinDB server|
|upload(variableObjectMap)|Upload local data to DolphinDB server|
|isBusy()|Judge if the current session is busy|
|close()|Close the current session|

For a detailed example, users can refer to the [example directory](https://github.com/dolphindb/api-java/tree/master/example).

### 2. Establish a DolphinDB connection

The Java API connects to the DolphinDB server via TCP/IP protocol. To connect to a local DolphinDB server with port number 8848:

```
import com.xxdb;
DBConnection conn = new DBConnection();
boolean success = conn.connect("localhost", 8848);
```
Establish a connection with a username and password:
```
boolean success = conn.connect("localhost", 8848, "admin", "123456");
```
If the connection is established without a username and password, we only have guest privileges. To be granted more privileges, we can log in by calling conn.login('admin', '123456', true).

### 3.Run script

Use the following statement to run DolphinDB script in Java:
```
conn.run("script");
```
The maximum length of the script is 65,535 bytes.

### 4. Execute functions

We can use method `run` to execute DolphinDB built-in functions or user-defined functions on a remote DolphinDB server.

The following examples illustrate 3 ways to call DolphinDB's built-in function `add` in Java, depending on the locations of the parameters "x" and "y" of function `add`.

* Both parameters are on DolphinDB server

If both variables "x" and "y" have been generated on DolphinDB server by Java applications,
```
conn.run("x = [1,3,5];y = [2,4,6]")
```
then we can execute run("script") directly.
```
public void testFunction() throws IOException{
    Vector result = (Vector)conn.run("add(x,y)");
    System.out.println(result.getString());
}
```

* Only 1 parameter exists on DolphinDB server

Parameter "x" was generated on DolphinDB server by the Java program, and parameter "y" is to be generated by the Java program.
```
conn.run("x = [1,3,5]")
```
In this case, we need to use "partial application" to embed parameter "x" in function `add`. For details, please refer to [Partial Application Documentation](https://www.dolphindb.com/cn/help/PartialApplication.html)。

```
public void testFunction() throws IOException{
    List<Entity> args = new ArrayList<Entity>(1);
    BasicDoubleVector y = new BasicDoubleVector(3);
    y.setDouble(0, 2.5);
    y.setDouble(1, 3.5);
    y.setDouble(2, 5);
    args.Add(y);
    Vector result = (Vector)conn.run("add{x}", args);
    System.out.println(result.getString());
}
```

* Both parameters are to be generated by Java program
```
import java.util.List;
import java.util.ArrayList;

public void testFunction() throws IOException{
    List<Entity> args = new ArrayList<Entity>(1);
    BasicDoubleVector x = new BasicDoubleVector(3);
    x.setDouble(0, 1.5);
    x.setDouble(1, 2.5);
    x.setDouble(2, 7);
    BasicDoubleVector y = new BasicDoubleVector(3);
    y.setDouble(0, 2.5);
    y.setDouble(1, 3.5);
    y.setDouble(2, 5);
    args.Add(x);
    args.Add(y);
    Vector result = (Vector)conn.run("add", args);
    System.out.println(result.getString());
}
```

### 5. Upload data objects

We can upload a binary data object to DolphinDB server and assign it to a variable for future use. Variable names can use 3 types of characters: letters, numbers and underscores. The first character must be a letter.

```
public void testFunction() throws IOException{
    Map<String, Entity> vars = new HashMap<String, Entity>();
    BasicDoubleVector vec = new BasicDoubleVector(3);
    vec.setDouble(0, 1.5);
    vec.setDouble(1, 2.5);
    vec.setDouble(2, 7);
    vars.put("a",vec);
    conn.upload(vars);
    Entity result = conn.run("accumulate(+,a)");
    System.out.println(result.getString());
}
```

### 6. Read data

This section introduces how to read different data forms in DolphinDB with the DBConnection object.

We need to import the DolphinDB data type package:

```
import com.xxdb.data.*;
```

- Vector

The following DolphinDB statement returns a Java object BasicStringVector. 

```
rand(`IBM`MSFT`GOOG`BIDU,10)
```

The `rows` method returns the size of a vector. We can access vector elements by index with the `getString` method.

```
public void testStringVector() throws IOException{
    BasicStringVector vector = (BasicStringVector)conn.run("rand(`IBM`MSFT`GOOG`BIDU, 10)");
    int size = vector.rows();
    System.out.println("size: "+size);
    for(int i=0; i<size; ++i)
        System.out.println(vector.getString(i));
}
```

Similarly, we can work with vectors or tuples of INT, DOUBLE, FLOAT or any other types.
```
public void testDoubleVector() throws IOException{
    BasicDoubleVector vector = (BasicDoubleVector)conn.run("rand(10.0, 10)");
    int size = vector.rows();
    System.out.println("size: "+size);
    for(int i=0; i<size; ++i)
       System.out.println(vector.getDouble(i));
}
```

```
public void testAnyVector() throws IOException{
    BasicAnyVector result = (BasicAnyVector)conn.run("[1, 2, [1,3,5],[0.9, [0.8]]]");
    System.out.println(result.getString());
}
```

- Set

```
public void testSet() throws IOException{
    BasicSet result = (BasicSet)conn.run("set(1+3*1..100)");
    System.out.println(result.getString());
}
```

- Matrix

To retrieve an element from an integer matrix, we can use `getInt`. To get the number of rows and columns of a matrix, we can use functions `rows` and `columns`.

```
public void testIntMatrix() throws IOException {
    BasicIntMatrix matrix = (BasicIntMatrix)conn.run("1..6$3:2");
    System.out.println(matrix.getString());
}
```

- Dictionary

The keys and values of a dictionary can be retrieved with functions `keys` and `values`, respectively. To get the value for a key, use `get`.

```
public void testDictionary() throws IOException{
    BasicDictionary dict = (BasicDictionary)conn.run("dict(1 2 3,`IBM`MSFT`GOOG)");
    //to print the corresponding value for key 1.
    System.out.println(dict.get(new BasicInt(1)).getString());
}
```

- Table

To get a column of a table, use `table.getColumn(index)`; to get a column name, use `table.getColumnName(index)`. To get the number of columns and rows of a table, use `table.columns()` and `table.rows()`, respectively.

```
public void testTable() throws IOException{
    StringBuilder sb =new StringBuilder();
    sb.append("n=2000\n");
    sb.append("syms=`IBM`C`MS`MSFT`JPM`ORCL\n");
    sb.append("mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms, n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price)\n");
    sb.append("select qty,price from mytrades where sym=`IBM");
    BasicTable table = (BasicTable)conn.run(sb.toString());
    System.out.println(table.getString());
}
```
- NULL object

To describe a NULL object, we can use `obj.getDataType()`.

```
public void testVoid() throws IOException{
    Entity obj = conn.run("NULL");
    System.out.println(obj.getDataType());
}
```

### 7. Read/write DolphinDB tables

Users may ingest data from other database systems or third-party APIs to a DolphinDB database. This section introduces how to upload and save data with Java API.

There are 3 types of DolphinDB tables:

- In-memory table: it has the fastest access speed, but if the node shuts down the data will be lost.
- Local disk table: data are saved on the local disk and can be loaded into memory.
- Distributed table: data are distributed across disks of multiple nodes. Users can query the table as if it is a local disk table.

#### 7.1 Save data to a DolphinDB in-memory table

DolphinDB offers several ways to save data to an in-memory table:
- Save a single row of data with `insert into`
- Save multiple rows of data in bulk with function `tableInsert`
- Save a table object with function `tableInsert`

It is not recommended to save data with function `append!`, as `append!` returns table schema that unnecessarily increases the network traffic.

The table in the following examples has 4 columns. Their data types are string, int, timestamp and double. The column names are cstring, cint, ctimestamp and cdouble, respectively.
```
t = table(10000:0,`cstring`cint`ctimestamp`cdouble,[STRING,INT,TIMESTAMP,DOUBLE])
share t as sharedTable
```
By default, an in-memory table is not shared among sessions. To access it in a different session, we need to share it among sessions with `share`.

##### 7.1.1 Save a single record to an in-memory table with 'insert into' 

```
public void test_save_Insert(String str,int i, long ts,double dbl) throws IOException{
    conn.run(String.format("insert into sharedTable values('%s',%s,%s,%s)",str,i,ts,dbl));
}
```

##### 7.1.2 Save data in batches with `tableInsert`

To save multiple records in batches, we can use `Arrays.asLIst` method to encapsulate multiple vectors in a List, then use function `tableInsert` to append it to a table.

```
public void test_save_TableInsert(List<String> strArray,List<Integer> intArray, List<Long> tsArray,List<Double> dblArray) throws IOException{
    //Construct parameters with arrays
    List<Entity> args = Arrays.asList(new BasicStringVector(strArray),new BasicIntVector(intArray),new BasicTimestampVector(tsArray),new BasicDoubleVector(dblArray));
    conn.run("tableInsert{sharedTable}", args);
}
```

The function `tableInsert` accepts a table object as input as well.

```
public void test_save_table(BasicTable table1) throws IOException {
    List<Entity> args = Arrays.asList(table1);
    conn.run("tableInsert{shareTable}", args);
}
```

The example above uses partial application in DolphinDB to embed a table in `tableInsert{sharedTable}` as a function. For details about partial application, please refer to [Partial Application Documentation](https://www.dolphindb.com/cn/help/PartialApplication.html).

##### 7.1.3 Use function `tableInsert` to save BasicTable objects

Function `tableInsert` can also accept a BasicTable object in Java as a parameter to append data to a table in batches. 

```
public void test_save_table(BasicTable table1) throws IOException {
    List<Entity> args = Arrays.asList(table1);
    conn.run("tableInsert{shareTable}", args);
}
```

#### 7.2 Save data to a distributed table

Distributed table is recommended by DolphinDB in production environment. It supports snapshot isolation and ensures data consistency. Distributed table supports multiple copy mechanism, which offers fault tolerance and load balancing.

Please note that distributed tables can only be used in cluster environments with `enableDFS=1` enabled.

```
dbPath = 'dfs://testDatabase'
tbName = 'tb1'

if(existsDatabase(dbPath)){dropDatabase(dbPath)}
db = database(dbPath,RANGE,2018.01.01..2018.12.31)
db.createPartitionedTable(t,tbName,'ctimestamp')
```
DolphinDB provides `loadTable` method to load distributed tables and `tableInsert` method to append data. 

```
public void test_save_table(String dbPath, BasicTable table1) throws IOException{
    List<Entity> args = new ArrayList<Entity>(1);
    args.add(table1);
    conn.run(String.format("tableInsert{loadTable('%s','tb1')}",dbPath), args);
}
```

When we retrieve an array or a list in the Java program, it is also convenient to construct a BasicTable for appending data. For example, if we have `boolArray, intArray, dblArray, dateArray, strArray` 5 list objects (List<T>), we can construct a BasicTable object:
```
List<String> colNames =  Arrays.asList("cbool","cint","cdouble","cdate","cstring");
List<Vector> cols = Arrays.asList(new BasicBooleanVector(boolArray),new BasicIntVector(intArray),new BasicDoubleVector(dblArray),new BasicDateVector(dateArray),new BasicStringVector(strArray));
BasicTable table1 = new BasicTable(colNames,cols);
```

#### 7.3 Save data to local disk table

Local disk tables can be used for data analysis on historical data sets. They do not support transactions, nor do they support concurrent reading and writing.

```
dbPath = "C:/data/testDatabase"
tbName = 'tb1'

if(existsDatabase(dbPath)){dropDatabase(dbPath)}
db = database(dbPath,RANGE,2018.01.01..2018.12.31)
db.createPartitionedTable(t,tbName,'ctimestamp')
```

DolphinDB provides `loadTable` method to load local disk tables, and function `tableInsert` to append data.

```
public void test_save_table(String dbPath, BasicTable table1) throws IOException{
    List<Entity> args = new ArrayList<Entity>(1);
    args.add(table1);
    conn.run(String.format("tableInsert{loadTable('%s','tb1')}",dbPath), args);
}
```
#### 7.4 Load table

In Java API, a table is saved as a BasicTable object. Since BasicTable is column based, to retrieve rows we need to get the necessary columns first and then get the rows.

In the example below, the BasicTable has 4 columns: STRING, INT, TIMESTAMP and DOUBLE. The column names are cstring, cint, ctimestamp and cdouble.

```
public void test_loop_basicTable(BasicTable table1) throws Exception{
    BasicStringVector stringv = (BasicStringVector) table1.getColumn("cstring");
    BasicIntVector intv = (BasicIntVector) table1.getColumn("cint");
    BasicTimestampVector timestampv = (BasicTimestampVector) table1.getColumn("ctimestamp");
    BasicDoubleVector doublev = (BasicDoubleVector) table1.getColumn("cdouble");
    for(int ri=0; ri<table1.rows(); ri++){
        System.out.println(stringv.getString(ri));
        System.out.println(intv.getInt(ri));
        LocalDateTime timestamp = timestampv.getTimestamp(ri);
        System.out.println(timestamp);
        System.out.println(doublev.getDouble(ri));
    }
}
```

### 8. Data type conversion between DolphinDB and Java

Java API provides objects that correspond to DolphinDB data types. They are usually named as Basic+ `<DataType>`, such as BasicInt, BasicDate, and so on.

For certain basic Java types, we can directly create the corresponding DolphinDB data types such as `new BasicInt(4)`, `new BasicDouble(1.23)`. The following Java types, however, need to be converted.
- `CHAR` type: The `CHAR` type in DolphinDB is stored as a Byte, so use the `BasicByte` type to construct `CHAR` in the Java API, for example `new BasicByte((byte)'c')`
- `SYMBOL` type: The `SYMBOL` type in DolphinDB is an optimization of strings, which can improve the efficiency of DolphinDB for string data storage and query, but this type is not needed in Java, so Java API does not provide `BasicSymbol `This kind of object can be processed directly with `BasicString`.
- Temporal types: Temporal data types are stored as int or long type in DolphinDB. DolphinDB provides 9 temporal data types: date, month, time, minute, second, datetime, timestamp, nanotime, nanotimestamp, the highest precision can be Nanoseconds. For a detailed description, refer to [DolphinDB Timing Type and Conversion] (https://www.dolphindb.com/cn/help/TemporalTypeandConversion.html). Since Java also provides data types such as LocalDate, LocalTime, LocalDateTime and YearMonth, Java API provides all Java temporal types and conversion functions between int or long in the Utils class.

The following script shows the correspondence between DolphinDB temporal types in Java API and Java native time types:

```
//Date:2018.11.12
BasicDate bd = new BasicDate(LocalDate.of(2018,11,12));
//Month:2018.11M
BasicMonth bm = new BasicMonth(YearMonth.of(2018,11));
//Time:20:08:01.123
BasicTime bt = new BasicTime(LocalTime.of(20,8,1,123000000));
//Minute:20:08m
BasicMinute bmn = new BasicMinute(LocalTime.of(20,8));
//Second:20:08:01
BasicSecond bs = new BasicSecond(LocalTime.of(20,8,1));
//DateTime: 2018.11.12T08:01:01
BasicDateTime bdt = new BasicDateTime(LocalDateTime.of(2018,11,12,8,1,1));
//Timestamp: 2018.11.12T08:01:01.123
BasicTimestamp bts = new BasicTimestamp(LocalDateTime.of(2018,11,12,8,1,1,123000000));
//NanoTime: 20:08:01.123456789
BasicNanoTime bnt = new BasicNanoTime(LocalTime.of(20,8,1,123456789));
//NanoTimestamp: 2018.11.12T20:08:01.123456789
BasicNanoTimestamp bnts = new BasicNanoTimestamp(LocalDateTime.of(2018,11,12,8,1,1,123456789))
```

If a temporal variable is stored as timestamp in a third-party system, DolphinDB time object can also be instantiated with a timestamp.
The Utils class in the Java API provides conversion algorithms for various temporal types and standard timestamps, such as converting millisecond timestamps to DolphinDB's `BasicTimestamp` objects:

```
LocalDateTime dt = Utils.parseTimestamp(1543494854000l);
BasicTimestamp ts = new BasicTimestamp(dt);
```

You can also convert a DolphinDB object to a timestamp of an integer or long integer, such as:
```
LocalDateTime dt = ts.getTimestamp();
long timestamp = Utils.countMilliseconds(dt);
```
If the timestamp is saved with other precision, the Utils class also provides the following methods to accommodate a variety of different precisions:
- Utils.countMonths: Calculate the monthly difference between a given time and 1970.01, returning an int
- Utils.countDays: Calculate the difference in the number of days between the given time and 1970.01.01, return int
- Utils.countMinutes: Calculate the minute difference between the given time and 1970.01.01T00:00, return int
- Utils.countSeconds: Calculate the difference in seconds between a given time and 1970.01.01T00:00:00, returning int
- Utils.countMilliseconds: Calculate the difference in milliseconds between a given time and 1970.01.01T00:00:00, return long
- Utils.countNanoseconds: Calculate the difference in nanoseconds between a given time and 1970.01.01T00:00:00.000, return long


