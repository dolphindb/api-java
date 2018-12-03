### 1. Java API Introduction
The Java API essentially implements the messaging and data conversion protocol between the Java program and the DolphinDB server.

It needs to run in a Java 1.8 or higher environment

The Java API follows the principles of interface-oriented programming. The Java API uses the interface class Entity to represent all the data types returned by DolphinDB. Based on the Entity interface class, according to the data type of DolphinDB, the Java API provides seven extension interfaces, namely scalar, vector, matrix, set, dictionary, table and chart. These interface classes are included in the com.xxdb.data package.

Extended interface classes | Naming rules | Examples
---|---|---
scalar|`Basic<DataType>`|BasicInt, BasicDouble, BasicDate, etc.
vector, matrix|`Basic<DataType><DataForm>`|BasicIntVector, BasicDoubleMatrix, BasicAnyVector, etc.
set, dictionary, and table|`Basic<DataForm>`|BasicSet, BasicDictionary, BasicTable.
chart||BasicChart

"Basic" represents the basic data type interface, `<DataType>` represents the DolphinDB data type name, and `<DataForm>` is a DolphinDB data form name.

For detailed interface and class description, please refer to [Java API Manual](https://www.dolphindb.com/javaapi/)

One of core functions provided by the DolphinDB Java API is DBConnection. Its main function is to allow Java applications to execute scripts and functions on the DolphinDB server and pass data between them in both directions.

The DBConnection class provides the following main methods:



| Method Name | Details |
|:------------- |:-------------|
|connect(host, port, [username, password])|Connect the session to the DolphinDB server|
|login(username,password,enableEncryption)|Login server|
|run(script)|Run the script on the DolphinDB server|
|run(functionName,args)|Call the function on the DolphinDB server|
|upload(variableObjectMap)|Upload local data objects to DolphinDB server|
|isBusy()|Judge if the current session is busy |
|close()|Close the current session|


### 2. Establish a DolphinDB connection

The Java API connects to the DolphinDB server via the TCP/IP protocol. In the following example, we connect the running local DolphinDB server with port number 8848:

```
import com.xxdb;
DBConnection conn = new DBConnection();
boolean success = conn.connect("localhost", 8848);
```
Establish a connection with a username and password:
```
boolean success = conn.connect("localhost", 8848, "admin", "123456");
```
When the connection is successful without the username and password, the script runs under the guest permission. If you need to upgrade the permissions in subsequent runs, you can log in to get the permission by calling `conn.login('admin', '123456', true)`.

### 3.Run a script

The syntax for running the DolphinDB script in Java is as follows:

```
conn.run("script");
```

The maximum length of the script is 65,535 bytes.


If the script contains only one statement, such as an expression, DolphinDB returns a data object; otherwise it returns a NULL object. If the script contains more than one statement, the last object will be returned. If the script contains an error or there is a network problem, it throws an IOException.


### 4. Run a function

Method `run` also supports DolphinDB built-in functions and user defined functions to run on remote DolphinDB server.


The following example shows how the Java program calls DolhinDB's `add` function. The `add` function has two parameters. The calling method will be different based on the location of the parameters. The following examples show the sample code in three cases:

* All parameters are on the DolphinDB Server side

The variables x, y have been generated on the server side in advance by the java program.
```
conn.run("x = [1,3,5];y = [2,4,6]")
```
Then in the Java side to add these two vectors, you only need to use the `run(script)` method directly.
```
public void testFunction() throws IOException{
    Vector result = (Vector)conn.run("add(x,y)");
    System.out.println(result.getString());
}
```


* Some parameters exist on the DolphinDB Server side

The variable x has been generated on the server side in advance by the java program, and the parameter y is to be generated on the Java client.
```
conn.run("x = [1,3,5]")
```
At this time, you need to use the "partial application" method to embed parameter x in the add function. For details, please refer to [Partial Application Documentation](https://www.dolphindb.com/cn/help/PartialApplication.html)。

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

* Both parameters are in the java client
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

### 5. Upload a data object

When some data in Java needs to be used frequently by the server, it is certainly not a good practice to upload it once per call. At this time, you can use the upload method to upload the data to the server and assign it to a variable. This variable can be reused on the server side.


We can upload the binary data object to the DolphinDB server and assign it to a variable for future use. Variable names can use three types of characters: letters, numbers, or underscores. The first character must be a letter.

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

### 6. Read data example

The following describes the different types of data objects read bthrough the DBConnection method.

First import the DolphinDB data type package:

```
import com.xxdb.data.*;
```


Note that the code below needs to be established after the connection is established.

- Vector

The example below shows the DolphinDB statement generating a random fast symbol vector with size as 10.

```
rand(`IBM`MSFT`GOOG`BIDU,10)
```

Returns the Java object BasicStringVector. The vector.rows() method gets the size of the vector. We can access vector elements by index using the vector.getString(i) method.

```
public void testStringVector() throws IOException{
    BasicStringVector vector = (BasicStringVector)conn.run("rand(`IBM`MSFT`GOOG`BIDU, 10)");
    int size = vector.rows();
    System.out.println("size: "+size);
    for(int i=0; i<size; ++i)
        System.out.println(vector.getString(i));
}
```

Similarly, you can also handle vectors or tuples of int,double,float, or any other types.
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

To retrieve an element from an integer matrix, we can use getInt(row, col). To get the number of rows and columns, we can use the functions rows() and columns().

```
public void testIntMatrix() throws IOException {
    BasicIntMatrix matrix = (BasicIntMatrix)conn.run("1..6$3:2");
    System.out.println(matrix.getString());
}
```

- Dictionary

All keys and values ​​can be retrieved from the dictionary using the functions keys() and values(). To get its value from a key, you can call get(key).

```
public void testDictionary() throws IOException{
    BasicDictionary dict = (BasicDictionary)conn.run("dict(1 2 3,`IBM`MSFT`GOOG)");
    //to print the corresponding value for key 1.
    System.out.println(dict.get(new BasicInt(1)).getString());
}
```


- Table


To get the column of the table, we can call table.getColumn(index); again, we can call table.getColumnName(index) to get the column name. For the number of columns and rows, we can call table.columns() and table.rows() respectively.

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


To describe a NULL object, we can call the function obj.getDataType().

```
public void testVoid() throws IOException{
    Entity obj = conn.run("NULL");
    System.out.println(obj.getDataType());
}
```




### 7. Read and write DolphinDB data table

An important scenario for using the Java API is that users fetch data from other database systems or third-party WebAPIs, clean the data and store it in the DolphinDB database. This section describes uploading and saving the data retrieved through the Java API.


The DolphinDB data table is divided into three types according to storage methods:

- In-memory table: The data is only stored in the memory of this node, and the access speed is the fastest, but the node shutdown data does not exist.
- Local disk table: The data is saved on the local disk. Even if the node is closed, it can be easily loaded from the disk into the memory through the script.
- Distributed tables: Data is distributed across different nodes. Through DolphinDB's distributed computing engine, users can query the table like a local table.

#### 7.1 Save data to DolphinDB in-memory table

DolphinDB offers several ways to save data:
- save a single piece of data by insert into ;
- Save multiple pieces of data in bulk via the tableInsert function;
- Save the table object with the append! function.


The difference between these methods is that the types of parameters received are different. In a specific business scenario, a single data point may be obtained from the data source, or may be a data set composed of multiple arrays or tables.

The following describes three examples of saving data. The data table used in the example has four columns, namely `string, int, timestamp, double`, and the column names are `cstring,cint,ctimestamp,cdouble`. The script is as follows:
```
t = table(10000:0,`cstring`cint`ctimestamp`cdouble,[STRING,INT,TIMESTAMP,DOUBLE])
share t as sharedTable
```

Since an in-memory table is session-isolated, only the current GUI session can see the table. If you need to access it in a different Java program or other terminal, you need to share the in-memory table between sessions through the `share` keyword.

##### 7.1.1 Saving single point data using SQL
If the Java program is to save a single data record to DolphinDB each time, you can save the data through the SQL statement (insert into).

```
public void test_save_Insert(String str,int i, long ts,double dbl) throws IOException{
    conn.run(String.format("insert into sharedTable values('%s',%s,%s,%s)",str,i,ts,dbl));
}
```


##### 7.1.2 Using the tableInsert function to save data in batches



If the data obtained by the Java program can be organized into a List mode, it is more suitable to use the tableInsert function. This function can accept multiple arrays as parameters and append the array to the data table.

```
public void test_save_TableInsert(List<String> strArray,List<Integer> intArray, List<Long> tsArray,List<Double> dblArray) throws IOException{
    //Construct parameters with arrays
    List<Entity> args = Arrays.asList(new BasicStringVector(strArray),new BasicIntVector(intArray),new BasicTimestampVector(tsArray),new BasicDoubleVector(dblArray));
    conn.run("tableInsert{sharedTable}", args);
}
```


In the actual application scenario, usually the Java program writes data to a table already existing on the server side. On the server side, a script such as `tableInsert(sharedTable, vec1, vec2, vec3...)` can be used. But in Java, when called with `conn.run("tableInsert", args)`, the first parameter of tableInsert is the object reference of the server table. It cannot be obtained in the Java program, so the conventional practice is to define a function in server to embed the sharedTable, such as

```
def saveData(v1,v2,v3,v4){tableInsert(sharedTable,v1,v2,v3,v4)}
```

Then, run the function through `conn.run("saveData", args)`. Although this achieves the goal,  for the Java program, one more server cal consumes more network resources.


In this example, using the `partial application' feature in DolphinDB, the server table name is embeded into tableInsert in the manner of `tableInsert{sharedTable}` and used as a stand-alone function. This way you don't need to use a custom function.


For specific documentation, please refer to [Partial Application Documentation](https://www.dolphindb.com/cn/help/PartialApplication.html)。

##### 7.1.3 Use append! Function to save data in batches

The append! function accepts a table object as a parameter and appends the data to the data table.

```
public void test_save_table(BasicTable table1) throws IOException {
    List<Entity> args = Arrays.asList(table1);
    conn.run("append!{shareTable}", args);
}
```
#### 7.2 Save data to a distributed table
Distributed table is the data storage method recommended by DolphinDB in production environment. It supports snapshot level transaction isolation and ensures data consistency. Distributed table supports multiple copy mechanism, which provides data fault tolerance and data access. Load balancing.


The data tables involved in this example can be built with the following script:


*Please note that distributed tables can only be used in cluster environments with `enableDFS=1` enabled. *

```
dbPath = 'dfs://testDatabase'
tbName = 'tb1'

if(existsDatabase(dbPath)){dropDatabase(dbPath)}
db = database(dbPath,RANGE,2018.01.01..2018.12.31)
db.createPartitionedTable(t,tbName,'ctimestamp')
```
DolphinDB provides the loadTable method to load distributed tables and append data via append!. The specific script examples are as follows:

```
public void test_save_table(String dbPath, BasicTable table1) throws IOException{
    List<Entity> args = new ArrayList<Entity>(1);
    args.add(table1);
    conn.run(String.format("append!{loadTable('%s','tb1')}",dbPath), args);
}
```


When the value retrieved by the user in the Java program is an array or a list, it is also convenient to construct a BasicTable for appending data. For example, there are now `boolArray, intArray, dblArray, dateArray, strArray` 5 list objects (List< T>), you can construct a BasicTable object with the following statement:
```
List<String> colNames =  Arrays.asList("cbool","cint","cdouble","cdate","cstring");
List<Vector> cols = Arrays.asList(new BasicBooleanVector(boolArray),new BasicIntVector(intArray),new BasicDoubleVector(dblArray),new BasicDateVector(dateArray),new BasicStringVector(strArray));
BasicTable table1 = new BasicTable(colNames,cols);
```

#### 7.3 Save data to local disk table

Local disk tables are commonly used for computational analysis of static data sets, either for data input or as a calculated output. It does not support transactions, and does not support concurrent reading and writing.

```
// Create a data table using the DolphinDB script
dbPath = "C:/data/testDatabase"
tbName = 'tb1'

if(existsDatabase(dbPath)){dropDatabase(dbPath)}
db = database(dbPath,RANGE,2018.01.01..2018.12.31)
db.createPartitionedTable(t,tbName,'ctimestamp')
```

DolphinDB provides the loadTable method to load local disk tables as well, and function append! to append data.

```
public void test_save_table(String dbPath, BasicTable table1) throws IOException{
    List<Entity> args = new ArrayList<Entity>(1);
    args.add(table1);
    conn.run(String.format("append!{loadTable('%s','tb1')}",dbPath), args);
}
```
#### 7.4 Load table


In the Java API, the table data is saved as a BasicTable object. Since the BasicTable is a columnar store, all the desultory needs to be read and used by retrieving the rows and retrieving the rows.

In the example, the parameter BasicTable has 4 columns, which are `STRING, INT, TIMESTAMP, DOUBLE`, and the column names are `cstring, cint, ctimestamp, cdouble`.


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

### 8. DolphinDB和Java之间的数据类型转换
Java API提供了与DolphinDB内部数据类型对应的对象，通常是以Basic+ `<DataType>` 这种方式命名，比如BasicInt，BasicDate等等。
一些Java的基础类型，可以通过构造函数直接创建对应的DOlphinDB数据结构，比如`new BasicInt(4)`，`new BasicDouble(1.23)`，但是也有一些类型需要做一些转换，下面列出需要做简单转换的类型：
- `CHAR`类型：DolphinDB中的`CHAR`类型以Byte形式保存，所以在Java API中用`BasicByte`类型来构造`CHAR`，例如`new BasicByte((byte)'c')`
- `SYMBOL`类型：DolphinDB中的`SYMBOL`类型是对字符串的优化，可以提高DolphinDB对字符串数据存储和查询的效率，但是Java中并不需要这种类型，所以Java API不提供`BasicSymbol`这种对象，直接用`BasicString`来处理即可。
- 时间类型：DolphinDB的时间类型是以整形或者长整形来描述的，DolphinDB提供`date, month, time, minute, second, datetime, timestamp, nanotime, nanotimestamp`九种类型的时间类型，最高精度可以到纳秒级。具体的描述可以参考[DolphinDB时序类型和转换](https://www.dolphindb.com/cn/help/TemporalTypeandConversion.html)。由于Java也提供了`LocalDate,LocalTime,LocalDateTime,YearMonth`等数据类型，所以Java API在Utils类里提供了所有Java时间类型和int或long之间的转换函数。
- 

### 8. Data type conversion between DolphinDB and Java
The Java API provides objects that correspond to the internal data types of DolphinDB, usually named after Basic+ `<DataType>`, such as BasicInt, BasicDate, and so on.
Some basic Java types, you can directly create the corresponding DOlphinDB data structure through the constructor, such as `new BasicInt(4)`, `new BasicDouble(1.23)`, but there are some types that need to be converted. The following list needs to be simple. Type of conversion:
- `CHAR` type: The `CHAR` type in DolphinDB is stored as a Byte, so use the `BasicByte` type to construct `CHAR` in the Java API, for example `new BasicByte((byte)'c')`
- `SYMBOL` type: The `SYMBOL` type in DolphinDB is an optimization of strings, which can improve the efficiency of DolphinDB for string data storage and query, but this type is not needed in Java, so Java API does not provide `BasicSymbol `This kind of object can be processed directly with `BasicString`.
- Temporal type: The Temporal data type is internal stored as int or long type. DolphinDB provides 9 temporal data types: date, month, time, minute, second, datetime, timestamp, nanotime, nanotimestamp`, the highest precision can be Nanoseconds. For a detailed description, refer to [DolphinDB Timing Type and Conversion] (https://www.dolphindb.com/cn/help/TemporalTypeandConversion.html). Since Java also provides data types such as `LocalDate, LocalTime, LocalDateTime, YearMonth`, the Java API provides all Java temporal types and conversion functions between int or long in the Utils class.



The following script shows the correspondence between the DolphinDB time type in the Java API and the Java native time type:

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

If the time is stored in a timestamp in a third-party system, the DolphinDB time object can also be instantiated with a timestamp.
The Utils class in the Java API provides conversion algorithms for various time types and standard timestamps, such as converting millisecond timestamps to DolphinDB's `BasicTimestamp` objects:


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


