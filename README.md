This tutorial covers the following topics:
- [1. Java API Introduction](https://2xdb.net/dolphindb/api-java/-/blob/master/README.md#1-java-api-introduction)
- [2. Establish DolphinDB connection](https://2xdb.net/dolphindb/api-java/-/blob/master/README.md#2-establish-dolphindb-connection)
- [3. Run DolphinDB script](https://2xdb.net/dolphindb/api-java/-/blob/master/README.md#3-run-dolphindb-script)
- [4. Execute DolphinDB functions](https://2xdb.net/dolphindb/api-java/-/blob/master/README.md#4-execute-dolphindb-functions)
- [5. Upload data to DolphinDB server](https://2xdb.net/dolphindb/api-java/-/blob/master/README.md#5-upload-data-to-dolphindb-server)
- [6. Read data](https://2xdb.net/dolphindb/api-java/-/blob/master/README.md#6-read-data)
- [7. Read from and write to DolphinDB tables](https://2xdb.net/dolphindb/api-java/-/blob/master/README.md#7-read-from-and-write-to-dolphindb-tables)
  - [7.1 Save data to a DolphinDB in-memory table](https://2xdb.net/dolphindb/api-java/-/blob/master/README.md#71-save-data-to-a-dolphindb-in-memory-table)
    - [7.1.1 Save a single record to an in-memory table with 'insert into'](https://2xdb.net/dolphindb/api-java/-/blob/master/README.md#711-save-a-single-record-to-an-in-memory-table-with-insert-into)
    - [7.1.2 Save data in batches with tableInsert](https://2xdb.net/dolphindb/api-java/-/blob/master/README.md#712-save-data-in-batches-with-tableinsert)
    - [7.1.3 Use function tableInsert to save BasicTable objects](https://2xdb.net/dolphindb/api-java/-/blob/master/README.md#713-use-function-tableinsert-to-save-basictable-objects)
  - [7.2 Save data to a distributed table](https://2xdb.net/dolphindb/api-java/-/blob/master/README.md#72-save-data-to-a-distributed-table)
  - [7.3 Save data to a local disk table](https://2xdb.net/dolphindb/api-java/-/blob/master/README.md#73-save-data-to-a-local-disk-table)
  - [7.4 Load tables](https://2xdb.net/dolphindb/api-java/-/blob/master/README.md#74-load-tables)
- [8. Convert Java data types into DolphinDB data types](https://2xdb.net/dolphindb/api-java/-/blob/master/README.md#8-convert-java-data-types-into-dolphindb-data-types)
- [9. Java Streaming API](https://2xdb.net/dolphindb/api-java/-/blob/master/README.md#9-java-streaming-api)

### 1. Java API Introduction

DolphinDB Java API requires Java 1.8 or higher environment.

Java API adopts interface-oriented programming. It uses the interface "Entity" to represent all data types returned from DolphinDB. Java API provides 7 types of extended interfaces: scalar, vector, matrix, set, dictionary, table and chart based on the "Entity" interface and DolphinDB data forms. They are included in the package of com.xxdb.data.

Extended interface classes | Naming rules | Examples
---|---|---
scalar|Basic\<DataType\>|BasicInt, BasicDouble, BasicDate, etc.
vector, matrix|Basic\<DataType\>\<DataForm\>|BasicIntVector, BasicDoubleMatrix, BasicAnyVector, etc.
set, dictionary, table|Basic\<DataForm\>|BasicSet, BasicDictionary, BasicTable.
chart||BasicChart

"Basic" indicates the basic implementation of a data form interface, \<DataType\> indicates a DolphinDB data type, and \<DataForm\> indicates a DolphinDB data form. 

The most important object provided by DolphinDB Java API is DBConnection. It allows Java applications to execute script and functions on DolphinDB servers and transfer data between Java applications and DolphinDB servers in both directions. The DBConnection class provides the following main methods:

| Method Name | Details |
|:------------- |:-------------|
|connect(host, port, [username, password])|Connect the session to DolphinDB server|
|login(username,password,enableEncryption)|Log in to DolphinDB server|
|run(script)|Run script on DolphinDB server|
|run(functionName,args)|Call a function on DolphinDB server|
|upload(variableObjectMap)|Upload local data to DolphinDB server|
|isBusy()|Determine if the current session is busy|
|close()|Close the current session|

For a detailed example, users can refer to the [example directory](https://github.com/dolphindb/api-java/tree/master/example).

### 2. Establish DolphinDB connection

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

### 3. Run DolphinDB script

To run DolphinDB script in Java:
```
conn.run("script");
```
Please refer to the following section for an example.

### 4. Execute DolphinDB functions

Other than running script, method `run` can also execute DolphinDB built-in functions or user-defined functions on a remote DolphinDB server. If method `run` has only one parameter, the parameter is script. If method `run` has 2 parameters, the first parameter is a DolphinDB function name and the second parameter is the function's parameters.

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
In this case, we need to use "partial application" to embed parameter "x" in function `add`. For details, please refer to [Partial Application Documentation](https://www.dolphindb.com/help/Functionalprogramming/PartialApplication.html)。

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

### 5. Upload data to DolphinDB server

We can upload a data object to DolphinDB server and assign it to a variable for future use. Variable names can use 3 types of characters: letters, numbers and underscores. The first character must be a letter.

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

For the tuple [`GS, 2, [1,3,5],[0.9, [0.8]]], the following script gets the data form, data type and contents of the third element: 
```java
public void testAnyVector() throws IOException{
    
    BasicAnyVector result = (BasicAnyVector)conn.run("[`GS, 2, [1,3,5],[0.9, [0.8]]]");
    
    System.out.println(result.getEntity(2).getDataForm()); //DF_VECTOR
	System.out.println(result.getEntity(2).getDataType()); //DT_INT
	System.out.println(result.getEntity(2).getString()); //"[1,3,5]"
	System.out.println(((BasicIntVector)result.getEntity(2)).getInt(0)); //1
	System.out.println(((BasicIntVector)result.getEntity(2)).getInt(1)); //3
	System.out.println(((BasicIntVector)result.getEntity(2)).getInt(2)); //5
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

To retrieve an element from an integer matrix, we can use `getInt`. To get the number of rows and columns of a matrix, we can use functions `rows` and `columns`, respectively.

```java
public void testIntMatrix() throws IOException {
	//1..6$3:2
	//------
	//  1  4
	//  2  5
	//  3  6
	BasicIntMatrix matrix = (BasicIntMatrix)conn.run("1..6$3:2");
	System.out.println(matrix.getInt(0,1)==4);
	System.out.println(matrix.rows()==3);
	System.out.println(matrix.columns()==2);
}
```

- Dictionary

The keys and values of a dictionary can be retrieved with functions `keys` and `values`, respectively. To get the value for a key, use `get`.

```java
public void testDictionary() throws IOException{
		BasicDictionary dict = (BasicDictionary)conn.run("dict(1 2 3,`IBM`MSFT`GOOG)");
        System.out.println(dict.keys());  //[1, 2, 3]
		System.out.println(dict.values()); //[IBM, MSFT, GOOG]
		//to print the corresponding value for key 1.
		System.out.println(dict.get(new BasicInt(1)).getString()); //IBM
}
```

- Table

To get a column of a table, use `getColumn`; to get a column name, use `getColumnName`. To get the number of columns and rows of a table, use `columns` and `rows`, respectively.

```java
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

To determine if an object is NULL, use `getDataType`.

```java
public void testVoid() throws IOException{
	Entity obj = conn.run("NULL");
	System.out.println(obj.getDataType().equals(Entity.DATA_TYPE.DT_VOID)); //true
}
```

### 7. Read from and write to DolphinDB tables

There are 3 types of DolphinDB tables:

- In-memory table: it has the fastest access speed, but if the node shuts down the data will be lost.
- Local disk table: data are saved on the local disk and can be loaded into memory.
- Distributed table: data are distributed across disks of multiple nodes. Users can query the table as if it is a local disk table.

#### 7.1 Save data to a DolphinDB in-memory table

DolphinDB offers several ways to save data to an in-memory table:
- Save a single row of data with `insert into`
- Save multiple rows of data in bulk with function `tableInsert`
- Save a table object with function `tableInsert`

It is not recommended to save data with function `append!`, as `append!` returns the schema of a table and unnecessarily increases the network traffic.

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

Function `tableInsert` can save records in batches. If data in Java can be organized as a List, it can be saved with function `tableInsert`.

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

The example above uses partial application in DolphinDB to embed a table in `tableInsert{sharedTable}` as a function. For details about partial application, please refer to [Partial Application Documentation](https://www.dolphindb.com/help/Functionalprogramming/PartialApplication.html).

##### 7.1.3 Use function `tableInsert` to save BasicTable objects

Function `tableInsert` can also accept a BasicTable object in Java as a parameter to append data to a table in batches. 

```
public void test_save_table(BasicTable table1) throws IOException {
    List<Entity> args = Arrays.asList(table1);
    conn.run("tableInsert{shareTable}", args);
}
```

#### 7.2 Save data to a distributed table

Distributed table is recommended by DolphinDB in production environment. It supports snapshot isolation and ensures data consistency. With data replication, Distributed tables offers fault tolerance and load balancing.

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

We can conveniently construct a BasicTable with arrays or lists in Java to be appended to distributed tables. For example, if we have the following 5 list objects boolArray, intArray, dblArray, dateArray and strArray (List\<T\>), we can construct a BasicTable object:
```
List<String> colNames =  Arrays.asList("cbool","cint","cdouble","cdate","cstring");
List<Vector> cols = Arrays.asList(new BasicBooleanVector(boolArray),new BasicIntVector(intArray),new BasicDoubleVector(dblArray),new BasicDateVector(dateArray),new BasicStringVector(strArray));
BasicTable table1 = new BasicTable(colNames,cols);
```

#### 7.3 Save data to a local disk table

Local disk tables can be used for data analysis on historical data sets. They do not support transactions, nor do they support concurrent read and write.

```
dbPath = "C:/data/testDatabase"
tbName = 'tb1'

if(existsDatabase(dbPath)){dropDatabase(dbPath)}
db = database(dbPath,RANGE,2018.01.01..2018.12.31)
db.createPartitionedTable(t,tbName,'ctimestamp')
```

DolphinDB provides `loadTable` method to load local disk tables, and `tableInsert` method to append data.

```
public void test_save_table(String dbPath, BasicTable table1) throws IOException{
    List<Entity> args = new ArrayList<Entity>(1);
    args.add(table1);
    conn.run(String.format("tableInsert{loadTable('%s','tb1')}",dbPath), args);
}
```
#### 7.4 Load tables

In Java API, a table is saved as a BasicTable object. Since BasicTable is column based, to retrieve rows we need to get the necessary columns first and then get the rows.

In the example below, the BasicTable has 4 columns with data types STRING, INT, TIMESTAMP and DOUBLE. The column names are cstring, cint, ctimestamp and cdouble.

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

### 8. Convert Java data types into DolphinDB data types

Java API provides objects that correspond to DolphinDB data types. They are usually named as Basic+ \<DataType\>, such as BasicInt, BasicDate, etc.

The majority of DolphinDB data types can be constructed from corresponding Java data types. For examples, INT in DolphinDB from 'new BasicInt(4)', DOUBLE in DolphinDB from 'new BasicDouble(1.23)'. The following DolphinDB data types, however, need to be constructed in different ways: 

- CHAR type: as the CHAR type in DolphinDB is stored as a byte, we can use the BasicByte type to construct CHAR in Java API, for example 'new BasicByte((byte)'c')'.
- SYMBOL type: the SYMBOL type in DolphinDB is stored as INT to improve the efficiency of storage and query of strings. Java doesn't have this data type, so Java API does not provide BasicSymbol. SYMBOL type can be processed directly with BasicString. 
- Temporal types: temporal data types are stored as INT or LONG in DolphinDB. DolphinDB provides 9 temporal data types: date, month, time, minute, second, datetime, timestamp, nanotime and nanotimestamp. For detailed description, please refer to [DolphinDB Temporal Type and Conversion](https://www.dolphindb.com/help/DataManipulation/TemporalObjects/TemporalTypeandConversion.html). Since Java also provides data types such as LocalDate, LocalTime, LocalDateTime and YearMonth, Java API provides conversion functions in the Utils class between all Java temporal types and INT or LONG.

The following script shows the correspondence between DolphinDB temporal types and Java native temporal types:

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

If a temporal variable is stored as timestamp in a third-party system, DolphinDB temporal object can also be instantiated with a timestamp. The Utils class in the Java API provides conversion algorithms for various temporal types and standard timestamps, such as converting millisecond timestamps to DolphinDB's BasicTimestamp objects:

```
LocalDateTime dt = Utils.parseTimestamp(1543494854000l);
BasicTimestamp ts = new BasicTimestamp(dt);
```

We can also convert a DolphinDB object to a timestamp of an integer or long integer. For examples:
```
LocalDateTime dt = ts.getTimestamp();
long timestamp = Utils.countMilliseconds(dt);
```
The Utils class provides the following methods to handle a variety of timestamp precisions:
- Utils.countMonths: calculate the monthly difference between a given time and 1970.01, returning INT.
- Utils.countDays: calculate the difference in the number of days between the given time and 1970.01.01, returning INT.
- Utils.countMinutes: calculate the minute difference between the given time and 1970.01.01T00:00, returning INT.
- Utils.countSeconds: calculate the difference in seconds between a given time and 1970.01.01T00:00:00, returning INT.
- Utils.countMilliseconds: calculate the difference in milliseconds between a given time and 1970.01.01T00:00:00, returning LONG.
- Utils.countNanoseconds: calculate the difference in nanoseconds between a given time and 1970.01.01T00:00:00.000, returning LONG.

### 9. Java Streaming API

A Java program can subscribe to streaming data via API. Java API can acquire streaming data in the following 2 ways:

- The application on the client periodically checks if new data has been added to the streaming table. If yes, the application will acquire and consume the new data. 

```
PollingClient client = new PollingClient(subscribePort);
TopicPoller poller1 = client.subscribe(serverIP, serverPort, tableName, offset);

while (true) {
   ArrayList<IMessage> msgs = poller1.poll(1000);
   if (msgs.size() > 0) {
         BasicInt value = msgs.get(0).getEntity(2);  //get the element in the first row and the third column
   }
}
```

After poller1 detects that new data is added to the streaming table, it will pull the new data. When there is no new data, the Java program is waiting at poller1.poll method. 

- The API uses MessageHandler to get new data

First we need to define the message handler, which needs to implement com.xxdb.streaming.client.MessageHandle interface. 

```
public class MyHandler implements MessageHandler {
       public void doEvent(IMessage msg) {
               BasicInt qty = msg.getValue(2);
               //..data processing...
       }
}
```

The handler instance is passed into function `subscribe` as a parameter. 

```
ThreadedClient client = new ThreadedClient(subscribePort);
client.subscribe(serverIP, serverPort, tableName, new MyHandler(), offsetInt);
```

When new data is added to the streaming table, the system notifies Java API to use 'MyHandler' method to acquire the new data. 

#### Reconnect

Parameter reconnect is a Boolean value indicating whether to automatically resubscribe after the subscription experiences an expected interruption. The default value is false. 

If reconnect=true, whether and how the system resumes the subscription depends on how the unexpected interruption of subscription is caused. 

- If the publisher and the subscriber both stay on but the network connection is interrupted, then after network connection is restored, the subscriber resumes subscription from where the network interruption occurs. 
- If the publisher crashes, the subscriber will keep attempting to resume subscription after the publisher restarts. 
    - If persistence was enabled on the publisher, the publisher starts to read the persisted data on disk after restarting. The subscriber can't successfully resubscribe automatically until the publisher has read the data for the time when the publisher crashed.
    - If persistence was not enabled on the publisher, the subscriber will fail to automatically resubscribe. 
- If the subscriber crashes, the subscriber won't automatically resume the subscription after it restarts. In this case, we need to execute function `subscribe` again.

Parameter 'reconnect' is set to be true for the following example：

```
PollingClient client = new PollingClient(subscribePort);
TopicPoller poller1 = client.subscribe(serverIP, serverPort, tableName, offset, true);
```

#### filter

Parameter 'filter' is a vector. It is used together with function `setStreamTableFilterColumn` at the publisher node. Function `setStreamTableFilterColumn` specifies the filtering column in the streaming table. Only the rows with filtering column values in 'filter' are published. 

In the following example, parameter 'filter' is assigned an INT vector [1,2]: 

```
BasicIntVector filter = new BasicIntVector(2);
filter.setInt(0, 1);
filter.setInt(1, 2);

PollingClient client = new PollingClient(subscribePort);
TopicPoller poller1 = client.subscribe(serverIP, serverPort, tableName, actionName, offset, filter);
```
