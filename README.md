# DolphinDB Java API

**Note: This README documents DolphinDB Java API versions prior to 3.00.0.0. As of version 3.00.0.0, this README is no longer maintained. For documentation on the latest DolphinDB Java API, please refer to [DolphinDB Documentation](https://docs.dolphindb.com/en/javadoc/overview.html).**

---

- [DolphinDB Java API](#dolphindb-java-api)
  - [1. Introduction](#1-introduction)
  - [2. Establish DolphinDB Connection](#2-establish-dolphindb-connection)
    - [2.1. DBConnection](#21-dbconnection)
    - [2.2. SimpleDBConnectionPool](#22-simpledbconnectionpool)
    - [2.3. ExclusiveDBConnectionPool](#23-exclusivedbconnectionpool)
  - [3. Run DolphinDB Scripts](#3-run-dolphindb-scripts)
  - [4. Execute DolphinDB Functions](#4-execute-dolphindb-functions)
  - [5. Upload Data to DolphinDB Server](#5-upload-data-to-dolphindb-server)
  - [6. Read Data](#6-read-data)
  - [7. Read From and Write to DolphinDB Tables](#7-read-from-and-write-to-dolphindb-tables)
    - [7.1. Write to an In-Memory Table](#71-write-to-an-in-memory-table)
    - [7.2. Write to a DFS Table](#72-write-to-a-dfs-table)
    - [7.3. Load and Query Tables](#73-load-and-query-tables)
    - [7.4. Append Data Asynchronously](#74-append-data-asynchronously)
  - [8. Data Type Conversion](#8-data-type-conversion)
  - [9. Java Streaming API](#9-java-streaming-api)
    - [9.1. Interfaces](#91-interfaces)
    - [9.2. Code Examples](#92-code-examples)
    - [9.3. Reconnect](#93-reconnect)
    - [9.4. Filter](#94-filter)
    - [9.5. Subscribe to a Heterogeneous Table](#95-subscribe-to-a-heterogeneous-table)
    - [9.6. Unsubscribe](#96-unsubscribe)

## 1. Introduction

DolphinDB Java API requires Java 1.8 or higher environment. Please first declare the following Maven Dependency (version 3.00.0.0 in this example) in your project.

```java
<!-- https://mvnrepository.com/artifact/com.dolphindb/dolphindb-javaapi -->
<dependency>
    <groupId>com.dolphindb</groupId>
    <artifactId>dolphindb-javaapi</artifactId>
    <version>3.00.0.0</version>
</dependency>
```

As of 3.00.0.0, the method `Utils.getJavaApiVersion()` is provided to get the current Java API version.

Java API adopts interface-oriented programming. It uses the interface "Entity" to represent all data types returned from DolphinDB. Based on "Entity" and DolphinDB data forms, Java API provides the following types of extended interfaces: scalar, vector, matrix, set, dictionary, table, and chart. They are included in the package of com.xxdb.data.

| Extended interface classes | Naming rules              | Examples                                                |
| :------------------------- | :------------------------ | :------------------------------------------------------ |
| scalar                     | Basic\<DataType>           | BasicInt, BasicDouble, BasicDate, etc.                  |
| vector, matrix             | Basic\<DataType>\<DataForm> | BasicIntVector, BasicDoubleMatrix, BasicAnyVector, etc. |
| set, dictionary, table     | Basic\<DataForm>           | BasicSet, BasicDictionary, BasicTable                  |
| chart                      |                           | BasicChart                                              |

"Basic" indicates the basic implementation of a data form interface, \<DataType\> indicates a DolphinDB data type, and \<DataForm\> indicates a DolphinDB data form.

The most important object provided by DolphinDB Java API is `DBConnection`. It allows Java applications to execute scripts and functions on DolphinDB servers and transfer data between Java applications and DolphinDB servers in both directions. The DBConnection class provides the following main methods:

| Method Name                                                  | Details                                  |
| :----------------------------------------------------------- | :--------------------------------------- |
| DBConnection( \[asynchronousTask, useSSL, compress, usePython, sqlStd\]) | Construct an object                      |
| connect(host, port, \[username, password, initialScript, enableHighAvailability, highAvailabilitySites, reconnect, enableLoadBalance\]) | Connect the session to DolphinDB server  |
| login(username,password,enableEncryption)                    | Log in to DolphinDB server               |
| run(script)                                                  | Run script on DolphinDB server           |
| run(functionName,args)                                       | Call a function on DolphinDB server      |
| upload(variableObjectMap)                                    | Upload local data to DolphinDB server    |
| isBusy()                                                     | Check if the current session is busy     |
| close()                                                      | Close the current session                |

The parameter _sqlStd_ of the constructor method `DBConnection` is an enumeration type, specifying the syntax to parse input SQL scripts. Since version 1.30.22.1, three parsing syntaxes are supported: DolphinDB (default), Oracle, and MySQL. You can select the syntax by inputting the `SqlStdEnum` enumeration type.

Example:

```java
DBConnection conn = new DBConnection(false, false, false, false, false, true, SqlStdEnum.DolphinDB);
```

Note: If the current session is no longer in use, Java API will automatically close the connection after a while. You can close the session by calling `close()` to release the connection. Otherwise, other sessions may be unable to connect to the server due to too many connections.

## 2. Establish DolphinDB Connection

### 2.1. DBConnection

The Java API connects to the DolphinDB server via TCP/IP protocol. To connect to a local DolphinDB server with port number 8848:

```java
import com.xxdb;
DBConnection conn = new DBConnection();
boolean success = conn.connect("localhost", 8848);
```

Starting from API 1.30.17.1, the following optional parameters can be specified for `connection`: *asynchronousTask*, *useSSL*, *compress*, and *usePython*. The default values of these parameters are false.

The following example establishes a connection to the server. It enables SSL and compression but disables asynchronous communication. Please note that the configuration parameter *enableHTTPS*=true must be specified on the server side.

```java
DBConnection conn = new DBConnection(false, true, true);
```

The following example establishes a connection to the server. It supports asynchronous communication where no values are returned for DolphinDB scripts and functions. It is ideal for asynchronous writes.

```java
DBConnection conn = new DBConnection(true, false);
```

Establish a connection with a username and password:

```java
boolean success = conn.connect("localhost", 8848, "admin", "123456");
```

If the connection is established without a username and password, you only have guest privileges. To be granted with more privileges, we can log in by executing `conn.login('admin', '123456', true)`.

To define and use user-defined functions in a Java program, you can pass in the user-defined scripts to the parameter initialScript. The advantages are:
(1) These functions don't need to be defined repeatedly every time `run` is called;
(2) The API client can automatically connect to the server after disconnection. If the parameter *initialScript* is specified, the Java API will automatically execute the script and register the functions. The parameter can be very useful for scenarios where the network is not stable but the program needs to run continuously.

```java
boolean success = conn.connect("localhost", 8848, "admin", "123456", "");
```

To enable high availability, set the parameter *enableHighAvailability* to true.

As of version 1.30.22.2, load balancing is automatically enabled for HA mode. Since 2.00.11.0, the `connect` method supports a new parameter *enableLoadBalance* which allows users to enable/disable load balancing in HA mode. Load balancing is only supported in HA mode and it is disabled by default.

If load balancing is disabled in HA mode, the API establishes connection to a random node. You can specify a group of nodes for connection for *highAvailabilitySites*, and the API will establish the connection to a random node from the group. If load balancing is enabled in HA mode, the API establishes connection to a low-load node. A low-load node is selected based on: memory usage<80%, connections<90%, and node load<80%.

**Note**: If a disconnection occurs, the API automatically reconnects following the above rules.

For example:
To enable high availability and load balancing before version 2.00.11.0:
```java
sites=["192.168.1.2:24120", "192.168.1.3:24120", "192.168.1.4:24120"]
boolean success = conn.connect("192.168.1.2", 24120, "admin", "123456", enableHighAvailability=true, highAvailabilitySites=sites);
```

To enable high availability and load balancing since version 2.00.11.0:
```java
boolean success = conn.connect("192.168.1.2", 24120, "admin", "123456", enableHighAvailability=true, highAvailabilitySites=sites, enableLoadBalance=true);
```

### 2.2. SimpleDBConnectionPool

Starting from version 2.00.11.1, the Java API provides connection pool `SimpleDBConnectionPool` for managing and reusing connections.

First configure the parameters with `SimpleDBConnectionPoolConfig`, and pass the `SimpleDBConnectionPoolConfig` as the configuration for `SimpleDBConnectionPool`. After the connection pool is constructed, users can obtain a connection with `getConnection`, and release a connection with `DBConnection.close()`. When a connection is returned to the pool, it becomes idle and can be utilized later.

#### 2.2.1. SimpleDBConnectionPoolConfig

The configuration can only be specified with `setXxx` method, for example:

```java
SimpleDBConnectionPoolConfig config = new SimpleDBConnectionPoolConfig();
        config.setHostName("1sss");
        config.setPort(PORT);
        config.setUserId("admin");
        config.setPassword("123456");
        config.setInitialPoolSize(5);
        config.setEnableHighAvailability(false);
```

The following parameters can be configured:

- hostName: IP address. The default value is localhost.
- port: Port number. The default value is 8848.
- userId: User ID. The default value is "".
- password: Password for the user. The default value is "". Only with both *userId* and *password* correctly specified can the connection log in the user. If only one of *userId* or *password* is specified, the connection will not log in the user. If the *userId* or *password* is incorrect, the connection fails.
- initialPoolSize: Initial pool size. The default value is 5.
- initialScript: Initial script. The default value is empty.
- compress: Whether to compress data when downloading. The default value is false. The compress mode is more suitable for queries on large volume of data. Transferring compressed data can save network cost, but increase workloads of compression and decompression.
- useSSL: Whether to enable SSL connection. The default value is false. To enable SSL connection, the configuration parameter *enableHTTPS* must be set to true on server.
- usePython: Whether to enable Python Parser. The default value is false.
- loadBalance: Whether to enable load balancing. The default value is false. If set to true, the API will perform load balancing in a polling manner across nodes specified in the *highAvailabilitySites* (or across all nodes if *highAvailabilitySites* is not specified).
- enableHighAvailability: Whether to enable high availability. The default value is false.
- highAvailabilitySites: A list of ip:port of all available nodes.

#### 2.2.2. Methods

The following methods are provided in the `SimpleDBConnectionPool` class:

| Method 	| Description 	|
|---	|---	|
| SimpleDBConnectionPool(simpleDBConnectionPoolConfig) 	| Constructor. 	|
| DBConnection getConnection() 	| Get a connection from the pool. 	|
| close() 	| Close a connection pool. 	|
| isClosed() 	| Check whether the pool is closed. 	|
| getActiveConnectionsCount() 	| Get the number of active connections. 	|
| getIdleConnectionsCount() 	| Get the number of idle connections. 	|
| getTotalConnectionsCount() 	| Get the size of the pool. 	|
| DBConnection.close() 	| Release a connection from the pool. 	|

**Note**: The `DBConnection.close()` method is specifically used to release a connection obtained by `getConnection`, which is different from close() of DBConnection that closes the current session.

```
// Configure the connection pool
SimpleDBConnectionPoolConfig config = new SimpleDBConnectionPoolConfig();
        config.setHostName("1sss");
        config.setPort(PORT);
        config.setUserId("admin");
        config.setPassword("123456");
        config.setInitialPoolSize(5);
        config.setEnableHighAvailability(false);
 
// Initialize a connection pool       
SimpleDBConnectionPool pool = new SimpleDBConnectionPool(config);

// Get a connection
DBConnection conn = pool.getConnection();
conn.run("..."); // Execute scripts

// Release the connection
conn.close();

// Get the number of active connections
int activeConns = pool.getActiveConnectionsCount();

// Get the number of idle connections
int idleConns = pool.getIdleConnectionsCount();

// Close the connection pool
pool.close();
```

### 2.3. ExclusiveDBConnectionPool

The Java API provides connection pool `ExclusiveDBConnectionPool`. Users can execute a task with `execute` and then obtain the results with `getResult` method of `BasicDBTask`.

| Method                                                       | Details                                                      |
| :----------------------------------------------------------- | :----------------------------------------------------------- |
| ExclusiveDBConnectionPool(string host, int port, string uid,string pwd, int count, bool loadBalance,bool enableHighAvailability, string\[\] highAvailabilitySites = null, string initialScript, bool compress = false, bool useSSL = false, bool usePython = false) | Constructor. The parameter count indicates the number of connections to be used. If loadBalance is set to true, different nodes are connected. |
| execute(IDBTask task)                                        | Execute the task.                                            |
| execute(List tasks)                                          | Execute tasks in batches.                                    |
| getConnectionCount()                                         | Get the number of connections.                               |
| shutdown()                                                   | Shut down the connection pool.                               |

**Note**: If the current `ExclusiveDBConnectionPool` is no longer in use, Java API will automatically close the connection after a while. To release the connection resources, call `shutdown()`upon the completion of thread tasks.

`BasicDBTask` wraps the functions and arguments to be executed.

| Method Name                           | Details                                                      |
| :------------------------------------ | :----------------------------------------------------------- |
| BasicDBTask(string script, List args) | script: the function to be executed; args: the arguments passed. |
| BasicDBTask(string script)            | The script to be executed.                                   |
| isSuccessful()                        | Check whether the task is executed successfully.             |
| getResult()                           | Get the execution results.                                   |
| getErrorMsg()                         | Get the error messages.                                      |

Build a connection pool with 10 connections.

```java
ExclusiveDBConnectionPool pool = new ExclusiveDBConnectionPool(hostName, port, userName, passWord, 10, false, false);
```

Create a task.

```java
BasicDBTask task = new BasicDBTask("1..10");
pool.execute(task);
```

Check whether the task is executed successfully. If successful, return the results; otherwise return the error messages.

```java
BasicIntVector data = null;
if (task.isSuccessful()) {
    data = (BasicIntVector)task.getResult();
} else {
    throw new Exception(task.getErrorMsg());
}
System.out.print(data.getString());
```

Output:

```
[1,2,3,4,5,6,7,8,9,10]
```

Create multiple tasks and call these tasks concurrently in ExclusiveDBConnectionPool.

```java
List<DBTask> tasks = new ArrayList<>();
for (int i = 0; i < 10; ++i){
    //call function log
    tasks.add(new BasicDBTask("log", Arrays.asList(data.get(i))));
}
pool.execute(tasks);
```

Check whether the task is executed successfully. If successful, return the results; otherwise return the error messages.

```java
List<Entity> result = new ArrayList<>();
for (int i = 0; i < 10; ++i)
{
    if (tasks.get(i).isSuccessful())
    {
        result.add(tasks.get(i).getResult());
    }
    else
    {
        throw new Exception(tasks.get(i).getErrorMsg());
    }
    System.out.println(result.get(i).getString());
}
```

Output:

```
0
0.693147
1.098612
1.386294
1.609438
1.791759
1.94591
2.079442
2.197225
2.302585
```

## 3. Run DolphinDB Scripts

To run DolphinDB script in Java:

```java
conn.run("script");
```

Before version 2.00.11.0, the `run` method automatically enables sequence number. The number is a LONG integer that represents the task sequence number for a client. If a write task fails, the task will be resubmitted. However, in cases like writing multiple tables at once, data loss may occur.

Since version 2.00.11.0, the `run` method supports a new parameter enableSeqNo which allows users to enable/disable the sequence number feature. For example:

```java
public Entity run(String script, ProgressListener listener, int priority, int 
parallelism, int fetchSize, boolean clearSessionMemory, String tableName, boolean enableSeqNo)
```

## 4. Execute DolphinDB Functions

Other than running script, method `run` can also execute DolphinDB built-in functions or user-defined functions on a remote DolphinDB server. If method `run` has only one parameter, the parameter is a script. If method `run` has 2 parameters, the first parameter is a DolphinDB function name and the second parameter is the function's arguments.

Note: Please make sure that there are no extra spaces before and after the *function* parameter, and the specified function must exist. Otherwise, the following error will occur when executing `DBConnection.run(String function, List<Entity> arguments)`:

```java
Server response: 'Can't recognize function name functionA ' function: 'functionA '
```

The following examples illustrate 3 ways to call DolphinDB's built-in function `add` in Java, depending on the locations of the parameters "x" and "y" of function `add`.

- Both parameters are on DolphinDB server

If both variables "x" and "y" have been generated on DolphinDB server by Java applications,

```java
conn.run("x = [1,3,5];y = [2,4,6]")
```

then we can execute run("script") directly.

```java
public void testFunction() throws IOException{
    Vector result = (Vector)conn.run("add(x,y)");
    System.out.println(result.getString());
}
```

- Only 1 parameter exists on DolphinDB server

Parameter "x" was generated on DolphinDB server by the Java program, and parameter "y" is to be generated by the Java program.

```java
conn.run("x = [1,3,5]")
```

In this case, we need to use "partial application" to embed parameter "x" in function `add`. For details, please refer to [Partial Application Documentation](https://www.dolphindb.com/help/Functionalprogramming/PartialApplication.html).

```java
public void testFunction() throws IOException{
    List<Entity> args = new ArrayList<Entity>(1);
    BasicDoubleVector y = new BasicDoubleVector(3);
    y.setDouble(0, 2.5);
    y.setDouble(1, 3.5);
    y.setDouble(2, 5);
    args.add(y);
    Vector result = (Vector)conn.run("add{x}", args);
    System.out.println(result.getString());
}
```

- Both parameters are to be generated by Java program

```java
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
    args.add(x);
    args.add(y);
    Vector result = (Vector)conn.run("add", args);
    System.out.println(result.getString());
}
```

Before version 2.00.11.0, the `run` method automatically enables sequence number. Since version 2.00.11.0, the `run` method supports a new parameter enableSeqNo which allows users to enable/disable the sequence number feature. For example:

```
public Entity run(String function, List<Entity> arguments, int priority, int parallelism, int fetchSize, boolean enableSeqNo)
```

## 5. Upload Data to DolphinDB Server

We can upload a data object to DolphinDB server and assign it to a variable for future use. Variable names can use 3 types of characters: letters, numbers and underscores. The first character must be a letter.

```java
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

## 6. Read Data

This section introduces how to read different DolphinDB data forms with the `DBConnection` object.

Import the DolphinDB data type package:

```
import com.xxdb.data.*;
```

- Vector

The following DolphinDB statement returns a Java object BasicStringVector.

```
rand(`IBM`MSFT`GOOG`BIDU,10)
```

The `rows` method returns the size of a vector. We can access an element by index with the `getString` method.

```java
public void testStringVector() throws IOException{
    BasicStringVector vector = (BasicStringVector)conn.run("rand(`IBM`MSFT`GOOG`BIDU, 10)");
    int size = vector.rows();
    System.out.println("size: "+size);
    for(int i=0; i<size; ++i)
        System.out.println(vector.getString(i));
}
```

Similarly, we can work with vectors or tuples of INT, DOUBLE, FLOAT or any other types.

```java
public void testDoubleVector() throws IOException{
    BasicDoubleVector vector = (BasicDoubleVector)conn.run("rand(10.0, 10)");
    int size = vector.rows();
    System.out.println("size: "+size);
    for(int i=0; i<size; ++i)
       System.out.println(vector.getDouble(i));
}
```

For the tuple \[`GS, 2, \[1,3,5\],\[0.9, \[0.8\]\]\], the following script gets the data form, data type and contents of the third element:

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

```java
public void testSet() throws IOException{
	BasicSet result = (BasicSet)conn.run("set(1..100)");
	System.out.println(result.rows()==100);
	System.out.println(((BasicInt)result.keys().get(0)).getInt()==1);
}
```

- Matrix

To retrieve an element from an integer matrix, we can use `getInt`. To get the number of rows and columns of a matrix, we can use functions `rows` and `columns`, respectively.

```java
public void testIntMatrix() throws IOException {
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

## 7. Read From and Write to DolphinDB Tables

There are 2 types of DolphinDB tables:

- In-memory table: it has the fastest access speed, but if the node shuts down the data will be lost.
- DFS table: data are distributed across disks of multiple nodes.

### 7.1. Write to an In-Memory Table

DolphinDB offers several ways to write to an in-memory table:

- Insert a single row of data with `insert into`
- Insert multiple rows of data in bulk with function `tableInsert`
- Insert a table object with function `tableInsert`

It is not recommended to save data with function `append!`, as `append!` returns the schema of a table and unnecessarily increases the network traffic.

The table in the following examples has 4 columns. Their data types are STRING, INT, TIMESTAMP and DOUBLE. The column names are cstring, cint, ctimestamp and cdouble, respectively.

```
t = table(10000:0,`cstring`cint`ctimestamp`cdouble,[STRING,INT,TIMESTAMP,DOUBLE])
share t as sharedTable
```

By default, an in-memory table is not shared among sessions. To access it in a different session, share it among sessions with `share`.

#### 7.1.1. Insert a Single Record with `insert into`

To insert a single record to a DolphinDB in-memory table, you can use the `insert into` statement.

```java
public void test_save_Insert(String str,int i, long ts,double dbl) throws IOException{
    conn.run(String.format("insert into sharedTable values('%s',%s,%s,%s)",str,i,ts,dbl));
}
```

#### 7.1.2. Insert Multiple Records in Bulk with `tableInsert`

Function `tableInsert` can save records in batches. If data in Java can be organized as a List, it can be saved with function `tableInsert`.

```java
public void test_save_TableInsert(List<String> strArray,List<Integer> intArray,List<Long> tsArray,List<Double> dblArray) throws IOException{
    //Construct parameters with arrays
    List<Entity> args = Arrays.asList(new BasicStringVector(strArray),new BasicIntVector(intArray),new BasicTimestampVector(tsArray),new BasicDoubleVector(dblArray));
    conn.run("tableInsert{sharedTable}", args);
}
```

The example above uses partial application in DolphinDB to embed a table in `tableInsert{sharedTable}` as a function. For details about partial application, please refer to [Partial Application Documentation](https://www.dolphindb.com/help/Functionalprogramming/PartialApplication.html).

#### 7.1.3. Save BasicTable Objects With Function `tableInsert`

Function `tableInsert` can also accept a BasicTable object in Java as a parameter to append data to a table in batches.

```java
public void test_save_table(BasicTable table1) throws IOException {
    List<Entity> args = Arrays.asList(table1);
    conn.run("tableInsert{shareTable}", args);
}
```

### 7.2. Write to a DFS Table

DFS table is recommended by DolphinDB in production environment. It supports snapshot isolation and ensures data consistency. With data replication, DFS tables offers fault tolerance and load balancing.

#### 7.2.1. Save BasicTable Objects With Function `tableInsert`


```java
dbPath = 'dfs://testDatabase'
tbName = 'tb1'

if(existsDatabase(dbPath)){dropDatabase(dbPath)}
db = database(dbPath,RANGE,2018.01.01..2018.12.31)
db.createPartitionedTable(t,tbName,'ctimestamp')
```

DolphinDB provides `loadTable` method to load DFS tables and `tableInsert` method to append data.

```java
public void test_save_table(String dbPath, BasicTable table1) throws IOException{
    List<Entity> args = new ArrayList<Entity>(1);
    args.add(table1);
    conn.run(String.format("tableInsert{loadTable('%s','tb1')}",dbPath), args);
}
```

You can conveniently construct a BasicTable object with arrays or lists in Java to be appended to DFS tables. For example, the following 5 list objects boolArray, intArray, dblArray, dateArray and strArray are used to construct a BasicTable object:

```java
List<String> colNames =  Arrays.asList("cbool","cint","cdouble","cdate","cstring");
List<Vector> cols = Arrays.asList(new BasicBooleanVector(boolArray),new BasicIntVector(intArray),new BasicDoubleVector(dblArray),new BasicDateVector(dateArray),new BasicStringVector(strArray));
BasicTable table1 = new BasicTable(colNames,cols);
```


#### 7.2.2. Append to DFS Tables

DolphinDB DFS tables support concurrent reads and writes. This section introduces how to write data concurrently to DolphinDB DFS tables in Java.

Please note that multiple writers are not allowed to write to one partition at the same time in DolphinDB. Therefore, please make sure that each thread writes to a different partition separately when the client uses multiple writer threads.

DolphinDB's Java API offers a convenient way to separate data by partition and write concurrently:

```java
public PartitionedTableAppender(String dbUrl, String tableName, String partitionColName, String appendFunction, DBConnectionPool pool)
```

- **dbUrl:** DFS database path
- **tableName:** DFS table name
- **partitionColName:** partitioning column
- **appendFunction:** (optional) a user-defined function. `tableInsert` is called by default.
- **pool:** connection pool for concurrent writes

```java
DBConnectionPool pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 3, true, true);
PartitionedTableAppender appender = new PartitionedTableAppender(dbUrl, tableName , "sym", pool);
```

The following script first creates a DFS database "dfs://DolphinDBUUID" and a partitioned table "device_status". The database uses a COMPO domain of VALUE-HASH-HASH.

```
t = table(timestamp(1..10)  as date,string(1..10) as sym)
db1=database(\"\",HASH,[DATETIME,10])
db2=database(\"\",HASH,[STRING,5])
if(existsDatabase(\"dfs://demohash\")){
    dropDatabase(\"dfs://demohash\")
}
db =database(\"dfs://demohash\",COMPO,[db2,db1])
pt = db.createPartitionedTable(t,`pt,`sym`date)
```

With DolphinDB server version 1.30 or above, you can write to DFS tables with the `PartitionedTableAppender` object in Java API. The user needs to first specify a connection pool, and the system obtains information about partitions before assigning the partitions to the connection pool for concurrent writes. A partition can only be written by one thread at a time. For example:

```java
DBConnectionPool conn = new ExclusiveDBConnectionPool(host, Integer.parseInt(port), "admin", "123456", Integer.parseInt(threadCount), false, false);

PartitionedTableAppender appender = new PartitionedTableAppender(dbPath, tableName, "gid", "saveGridData{'" + dbPath + "','" + tableName + "'}", conn);
BasicTable table1 = createTable();
appender.append(table1);                   
```

### 7.3. Load and Query Tables

#### 7.3.1. Load Tables

To load a DFS table in Java API, you can execute the following code to read the table as a whole.

```java
String dbPath = "dfs://testDatabase";
String tbName = "tb1";
DBConnection conn = new DBConnection();
conn.connect(SERVER, PORT, USER, PASSWORD);
BasicTable table = (BasicTable)conn.run(String.format("select * from loadTable('%s','%s') where cdate = 2017.05.03",dbPath,tbName));
```

Starting from DolphinDB server 1.20.5, you can also load a large table in blocks.

Set the parameter *fetchSize* of `run` method to specify the size of a block, and the method returns an EntityBlockReader object. Use the `read` method to read data in blocks.

```java
DBConnection conn = new DBConnection();
conn.connect(SERVER, PORT, USER, PASSWORD);
EntityBlockReader v = (EntityBlockReader)conn.run("table(1..22486 as id)",(ProgressListener) null,4,4,10000);
BasicTable data = (BasicTable)v.read();
while(v.hasNext()){
    BasicTable t = (BasicTable)v.read();
    data = data.combine(t);
}
```

When using the above method to read data in blocks, if not all blocks are read, please call the `skipAll` method to abort the reading before executing the subsequent code. Otherwise, data will be stuck in the socket buffer and the deserialization of the subsequent data will fail.

```java
  EntityBlockReader v = (EntityBlockReader)conn.run("table(1..12486 as id)",(ProgressListener) null,4,4,10000);
  BasicTable data = (BasicTable)v.read();
  v.skipAll();
  BasicTable t1 = (BasicTable)conn.run("table(1..100 as id1)");  
```

#### 7.3.2. Use BasicTable Object

In Java API, a table is saved as a BasicTable object. Since BasicTable is column-based, to retrieve rows via the Java API, you need to access the columns first and then get the rows.

In the example below, the BasicTable has 4 columns with data types STRING, INT, TIMESTAMP and DOUBLE. The column names are cstring, cint, ctimestamp and cdouble.

```java
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

### 7.4. Append Data Asynchronously

You can use methods of `MultithreadedTableWriter` class to asynchronously append data to a DolphinDB in-memory table, dimension table, or a DFS table. The class maintains a buffer queue. Even when the server is fully occupied with network I/O operations, the writing threads of the API client will not be blocked.

For asynchronous writes:

- When the API client submits the task to the buffer queue, the task is considered as completed.
- You can check the status with `getStatus`.

#### 7.4.1. MultithreadedTableWriter

`MultithreadedTableWriter` supports concurrent writes in multiple threads.

The methods of `MultithreadedTableWriter` object are introduced as follows:

```java
public MultithreadedTableWriter(String hostName, int port, String userId, String password,
    String dbName, String tableName, boolean useSSL,
    boolean enableHighAvailability, String[] highAvailabilitySites,
    int batchSize, float throttle,
    int threadCount, String partitionCol,
    int[] compressTypes, Mode mode, String[] pModeOption,
    boolean enableActualSendTime)
```

Parameters:

- **hostName**: host name
- **port**: port number
- **userId** / **password**: username and password
- **dbPath**: a STRING indicating the DFS database path. Leave it unspecified for an in-memory table.
- **tableName**: a STRING indicating the in-memory or DFS table name.

**Note:** For API 1.30.17 or lower versions, when writing to an in-memory table, please specify the in-memory table name for *dbPath* and leave *tableName* empty.

- **useSSL**: a Boolean value indicating whether to enable SSL. The default value is false.
- **enableHighAvailability**: a Boolean value indicating whether to enable high availability. The default value is false.
- **highAvailabilitySites**: a list of ip:port of all available nodes
- **batchSize**: an integer indicating the number of messages in batch processing. The default value is 1, meaning the server processes the data as soon as they are written. If it is greater than 1, only when the number of data reaches *batchSize*, the client will send the data to the server.
- **throttle**: a positive floating-point number indicating the waiting time (in seconds) before the server processes the incoming data if the number of data written from the client does not reach *batchSize*.
- **threadCount**: an integer indicating the number of working threads to be created. The default value is 1, indicating single-threaded process. It must be 1 for a dimension table.
- **partitionCol**: a STRING indicating the partitioning column. It is None by default, and only takes effect when *threadCount* is greater than 1. For a partitioned table, it must be the partitioning column; for a stream table, it must be a column name; for a dimension table, the parameter does not take effect.
- **compressMethods**: an array of the compression methods used for each column. If unspecified, the columns are not compressed. The compression methods (case-insensitive) include:
  - "Vector.COMPRESS_LZ4": LZ4 algorithm
  - "Vector.COMPRESS_DELTA": Delta-of-delta encoding
- **enableActualSendTime**: a Boolean value that specifies whether to record the send time for each message. Note that the last column of tableName must be of NANOTIMESTAMP type.

The following part introduces methods of `MultithreadedTableWriter` object.

(1) insert

```java
ErrorCodeInfo insert(Object... args)
```

Details:

Insert a single record. Return a class `ErrorCodeInfo` containing *errorCode* and *errorInfo*. If *errorCode* is not "", `MultithreadedTableWriter` has failed to insert the data, and *errorInfo* displays the error message.

The class `ErrorCodeInfo` provides methods `hasError()` and `succeed()` to check whether the data is written properly. `hasError()` returns true if an error occurred, false otherwise. `succeed()` returns true if the data is written successfully, false otherwise.

**Parameters:**

- **args**: a variable-length argument (varargs) indicating the record to be inserted.

**Examples:**

```java
ErrorCodeInfo pErrorInfo = multithreadedTableWriter_.insert(new Date(2022, 3, 23), "AAAAAAAB", random.nextInt() % 10000);
```


(2) getUnwrittenData

```java
List<List<Entity>> getUnwrittenData()
```

**Details:**

Return a nested list of data that has not been written to the server.

**Note:** Data obtained by this method will be released by `MultithreadedTableWriter`.

**Examples:**

```java
List<List<Entity>> unwrittenData = multithreadedTableWriter_.getUnwrittenData();
```


(3) insertUnwrittenData

```java
ErrorCodeInfo insertUnwrittenData(List<List<Entity>> records)
```

**Details:**

Insert unwritten data. The result is in the same format as `insert`. The difference is that `insertUnwrittenData` can insert multiple records at a time.

**Parameters:**

- **records**: the data that has not been written to the server. You can obtain the object with method `getUnwrittenData`.

**Examples:**

```java
ErrorCodeInfo ret = multithreadedTableWriter_.insertUnwrittenData(unwrittenData);
```

(4) getStatus

```java
Status getStatus()
```

**Details:**

Get the current status of the `MultithreadedTableWriter` object.

**Parameters:**

- **status**: the `MultithreadedTableWriter.Status` class with the following attributes and methods:

For example

```java
MultithreadedTableWriter.Status writeStatus = new MultithreadedTableWriter.Status();
writeStatus = multithreadedTableWriter_.getStatus();
```

**Attributes:**

- **isExiting:** whether the threads are exiting
- **errorCode:** error code
- **errorInfo:** error message
- **sentRows:** number of sent rows
- **unsentRows:** number of rows to be sent
- **sendFailedRows:** number of rows failed to be sent
- **threadStatus:** a list of the thread status
  - threadId: thread ID
  - sentRows: number of rows sent by the thread
  - unsentRows: number of rows to be sent by the thread
  - sendFailedRows: number of rows failed to be sent by the thread

**Methods:**

- `hasError()`: return true if an error occurred, false otherwise.
- `succeed()`: return true if the data is written successfully, false otherwise.


(5) waitForThreadCompletion

```java
waitForThreadCompletion()
```

**Details:**

After calling the method, `MultithreadedTableWriter` will wait until all working threads complete their tasks. If you call `insert` or `insertUnwrittenData` after the execution of `waitForThreadCompletion`, an error "thread is exiting" will be raised.

**Examples:**

```java
multithreadedTableWriter_.waitForThreadCompletion();
```

The methods of `MultithreadedTableWriter` are usually used in the following way:

```java
DBConnection conn= new DBConnection();
conn.connect(HOST, PORT, "admin", "123456");
Random random = new Random();
String script =
        "dbName = 'dfs://valuedb3'" +
                "if (exists(dbName))" +
                "{" +
                "dropDatabase(dbName);" +
                "}" +
                "datetest = table(1000:0,`date`symbol`id,[DATE, SYMBOL, LONG]);" +
                "db = database(directory= dbName, partitionType= HASH, partitionScheme=[INT, 10]);" +
                "pt = db.createPartitionedTable(datetest,'pdatetest','id');";
conn.run(script);
MultithreadedTableWriter multithreadedTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://valuedb3", "pdatetest",
        false, false, null, 10000, 1,
        5, "id", new int[]{Vector.COMPRESS_LZ4, Vector.COMPRESS_LZ4, Vector.COMPRESS_DELTA});
ErrorCodeInfo ret;
try
{
    //insert 100 rows of records with correct data types and column count
    for (int i = 0; i < 100; ++i)
    {
        ret = multithreadedTableWriter_.insert(new Date(2022, 3, 23), "AAAAAAAB", random.nextInt() % 10000);
    }
} 
catch (Exception e)
{   //MTW raises an exception
    System.out.println("MTW exit with exception {0}" + e.getMessage());
}

//wait until MTW finishes writing
multithreadedTableWriter_.waitForThreadCompletion();
MultithreadedTableWriter.Status writeStatus = new MultithreadedTableWriter.Status();
writeStatus = multithreadedTableWriter_.getStatus();
if (!writeStatus.errorInfo.equals(""))
{
    //if error occured in writing
    System.out.println("error in writing !");
}
System.out.println("writeStatus: {0}\n" + writeStatus.toString());
System.out.println(((BasicLong)conn.run("exec count(*) from pt")).getLong());
```

Output:

```
"""
      writeStatus: {0}
      errorCode     : 
      errorInfo     : 
      isExiting     : true
      sentRows      : 100
      unsentRows    : 0
      sendFailedRows: 0
      threadStatus  :
              threadId        sentRows      unsentRows  sendFailedRows
                    13              30               0               0
                    14              18               0               0
                    15              15               0               0
                    16              20               0               0
                    17              17               0               0
    
      100
"""
```

The above example calls method `writer.insert()` to write data to writer, and obtains the status with `writer.getStatus()`. Please note that the method `writer.waitForThreadCompletion()` will wait for `MultithreadedTableWriter` to finish the data writes, and then terminate all working threads with the last status retained. A new MTW object must be created to write data again.

As shown in the above example, `MultithreadedTableWriter` applies multiple threads to data conversion and writes. The API client also uses multiple threads to call `MultithreadedTableWriter`, and the implementation is thread-safe.

#### 7.4.2. Exceptions Raised by MultithreadedTableWriter

When calling method `insert` of class `MultithreadedTableWriter`:

If the inserted data type does match the data type of the corresponding column, `MultithreadedTableWriter` immediately returns an error message and prints a stack.

For example:

```java
DBConnection conn= new DBConnection();
conn.connect(HOST, PORT, "admin", "123456");
Random random = new Random();
String script =
        "dbName = 'dfs://valuedb3'" +
                "if (exists(dbName))" +
                "{" +
                "dropDatabase(dbName);" +
                "}" +
                "datetest = table(1000:0,`date`symbol`id,[DATE, SYMBOL, LONG]);" +
                "db = database(directory= dbName, partitionType= HASH, partitionScheme=[INT, 10]);" +
                "pt = db.createPartitionedTable(datetest,'pdatetest','id');";
conn.run(script);
MultithreadedTableWriter multithreadedTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://valuedb3", "pdatetest",
        false, false, null, 10000, 1,
        5, "id", new int[]{Vector.COMPRESS_LZ4, Vector.COMPRESS_LZ4, Vector.COMPRESS_DELTA});
ErrorCodeInfo ret;
//MTW returns the error message
ret = multithreadedTableWriter_.insert(new Date(2022, 3, 23), 222, random.nextInt() % 10000);
if (!ret.errorInfo.equals(""))
    System.out.println("insert wrong format data: {2}\n" + ret.toString());
```

Output:

```
"""
      java.lang.RuntimeException: Failed to insert data. Cannot convert int to DT_SYMBOL.
      	at com.xxdb.data.BasicEntityFactory.createScalar(BasicEntityFactory.java:795)
      	at com.xxdb.data.BasicEntityFactory.createScalar(BasicEntityFactory.java:505)
      	at com.xxdb.multithreadedtablewriter.MultithreadedTableWriter.insert(MultithreadedTableWriter.java:594)
      	at com.xxdb.BehaviorTest.testMul(BehaviorTest.java:89)
      	at com.xxdb.BehaviorTest.main(BehaviorTest.java:168)
        code=A1 info=Invalid object error java.lang.RuntimeException: Failed to insert data. Cannot convert int to DT_SYMBOL.
"""
```

If the number of inserted columns does not match that of the table when the method `insert` is called, the error message is thrown immediately.

For example:

```java
DBConnection conn= new DBConnection();
conn.connect(HOST, PORT, "admin", "123456");
Random random = new Random();
String script =
        "dbName = 'dfs://valuedb3'" +
                "if (exists(dbName))" +
                "{" +
                "dropDatabase(dbName);" +
                "}" +
                "datetest = table(1000:0,`date`symbol`id,[DATE, SYMBOL, LONG]);" +
                "db = database(directory= dbName, partitionType= HASH, partitionScheme=[INT, 10]);" +
                "pt = db.createPartitionedTable(datetest,'pdatetest','id');";
conn.run(script);
MultithreadedTableWriter multithreadedTableWriter_ = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://valuedb3", "pdatetest",
        false, false, null, 10000, 1,
        5, "id", new int[]{Vector.COMPRESS_LZ4, Vector.COMPRESS_LZ4, Vector.COMPRESS_DELTA});
ErrorCodeInfo ret;
//MTW returns the error message
ret = multithreadedTableWriter_.insert(new Date(2022, 3, 23), random.nextInt() % 10000);
if (!ret.errorInfo.equals(""))
    System.out.println("insert wrong format data: {3}\n" + ret.toString());
```

Output:

```
"""
    insert wrong format data: {3}
      code=A2 info=Column counts don't match.  
"""
```

If error occurs when `MultithreadedTableWriter` is writing data, all working threads exit. If you continue to write data  to the server, an exception will be thrown and no data can be written as the thread has been terminated. You can use `getUnwrittenData()` to obtain the unwritten data and then rewrite it with `insertUnwrittenData()`. Please note that a new MTW object must be created to write the unwritten data.

For example:

```java
List<List<Entity>> unwriterdata = new ArrayList<>();
unwriterdata = multithreadedTableWriter_.getUnwrittenData();
System.out.println("{5} unwriterdata: " + unwriterdata.size());
//create a new object of MTW
MultithreadedTableWriter newmultithreadedTableWriter = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://valuedb3", "pdatetest",
        false, false, null, 10000, 1,
        5, "id", new int[]{Vector.COMPRESS_LZ4, Vector.COMPRESS_LZ4, Vector.COMPRESS_DELTA});
try
{
    boolean writesuccess = true;
    //insert the unwritten data
    ret = newmultithreadedTableWriter.insertUnwrittenData(unwriterdata);
}
finally
{
    newmultithreadedTableWriter.waitForThreadCompletion();
    writeStatus = newmultithreadedTableWriter.getStatus();
    System.out.println("writeStatus: {6}\n" + writeStatus.toString());
}
```

Output:

```
"""
  {5} unwriterdata: 10 
  writeStatus: {6}
  errorCode     : 
  errorInfo     : 
  isExiting     : true
  sentRows      : 10
  unsentRows    : 0
  sendFailedRows: 0
  threadStatus  :
          threadId        sentRows      unsentRows  sendFailedRows
                23               3               0               0
                24               2               0               0
                25               1               0               0
                26               3               0               0
                27               1               0               0
"""
```

## 8. Data Type Conversion

Java API provides objects that correspond to DolphinDB data types. They are usually named as Basic+ \<DataType\>, such as BasicInt, BasicDate, etc.

Data Types

| Java Primitive Data Type 	| Example 	| Java API Data Type 	| Example 	| DolphinDB Data Type 	| Example 	|
|---	|---	|---	|---	|---	|---	|
| Boolean 	| Boolean var = true; 	| BasicBoolean 	| BasicBoolean basicBoolean = new BasicBoolean(true); 	| BOOL 	| 1b, 0b, true, false 	|
| Byte 	| byte number = 10; 	| BasicByte 	| BasicByte basicByte = new BasicByte((byte) 13); 	| CHAR 	| 'a', 97c 	|
| LocalDate 	| LocalDate specificDate = LocalDate.of(2023, 6, 30); 	| BasicDate 	| BasicDate basicDate = new BasicDate(LocalDate.of(2021, 12, 9)); 	| DATE 	| 2023.06.13 	|
| Calendar 	| // create a Calendar object with specified date and time<br>Calendar specificCalendar = Calendar.getInstance();<br>specificCalendar.set(2023, Calendar.JUNE, 30, 12, 0, 0); 	| BasicDate 	| BasicDate basicDate = new BasicDate(specificCalendar); 	| DATE 	| 2023.06.13 	|
|   	| same as above 	| BasicDateHour 	| Calendar calendar = Calendar.getInstance();<br>calendar.set(2022,0,31,2,2,2);<br>BasicDateHour date = new BasicDateHour(calendar); 	| DATEHOUR 	| 2012.06.13T13 	|
|   	| same as above 	| BasicDateTime 	| BasicDateTime basicDateTime = new BasicDateTime(new GregorianCalendar()); 	| DATETIME 	| 2012.06.13 13:30:10 or 2012.06.13T13:30:10 	|
|   	| same as above 	| BasicTime 	| BasicTime basicTime = new BasicTime(new GregorianCalendar()); 	| TIME 	| 13:30:10.008 	|
|   	| same as above 	| BasicTimestamp 	| BasicTimestamp basicTimestamp = new BasicTimestamp(new GregorianCalendar()); 	| TIMESTAMP 	| 2012.06.13 13:30:10.008 or 2012.06.13T13:30:10.008 	|
| LocalDateTime 	| LocalDateTime currentDateTime = LocalDateTime.now(); 	| BasicDateHour 	| BasicDateHour basicDateHour = new BasicDateHour(LocalDateTime.now()); 	| DATEHOUR 	| 2012.06.13T13 	|
|   	| same as above 	| BasicDateTime 	| BasicDateTime basicDateTime = new BasicDateTime(LocalDateTime.of(2000, 2, 2, 3, 2, 3, 2)); 	| DATETIME 	| 2012.06.13 13:30:10 or 2012.06.13T13:30:10 	|
|   	| same as above 	| BasicMinute 	| BasicMinute basicMinute = new BasicMinute(LocalTime.of(11, 40, 53)); 	| MINUTE 	| 13:30m 	|
|   	| same as above 	| BasicNanoTime 	| BasicNanoTime basicNanoTime = new BasicNanoTime(LocalDateTime.of(2000, 2, 2, 3, 2, 3, 2)); 	| NANOTIME 	| 13:30:10.008007006 	|
|   	| same as above 	| BasicNanoTimestamp 	| BasicNanoTimestamp bnts = new BasicNanoTimestamp(LocalDateTime.of(2018,11,12,8,1,1,123456789)); 	| NANOTIMESTAMP 	| 2012.06.13 13:30:10.008007006 or 2012.06.13T13:30:10.008007006 	|
|   	| same as above 	| BasicTimestamp 	| BasicTimestamp basicTimestamp = new BasicTimestamp(LocalDateTime.of(2000, 2, 2, 3, 2, 3, 2)); 	| TIMESTAMP 	| 2012.06.13 13:30:10.008 or 2012.06.13T13:30:10.008 	|
| BigDecimal 	| BigDecimal decimal = new BigDecimal("3.1415926899");<br>BigDecimal afterSetScale = decimal.setScale(9, RoundingMode.FLOOR); 	| BasicDecimal32 	| BasicDecimal32 basicDecimal32 = new BasicDecimal32(15645.00, 0); 	| DECIMAL32(S) 	| 3.1415926$DECIMAL32(3) 	|
| BigDecimal 	| BigDecimal decimal = new BigDecimal("3.1234567891234567891");BigDecimal afterSetScale = decimal.setScale(18, RoundingMode.FLOOR); 	| BasicDecimal64 	| BasicDecimal64 decimal64 = new BasicDecimal64(15645.00, 0); 	| DECIMAL64(S) 	| 3.1415926$DECIMAL64(3), , 3.141P 	|
| BigDecimal 	| BigDecimal decimal = new BigDecimal("3.123456789123456789123456789123456789123");BigDecimal afterSetScale = decimal.setScale(38, RoundingMode.FLOOR); 	| BasicDecimal128 	| BasicDecimal128 basicDecimal128 = new BasicDecimal128("15645.00", 2); 	| DECIMAL128(S) 	|   	|
| Double 	| Double number = Double.valueOf(3.14); 	| BasicDouble 	| BasicDouble basicDouble = new BasicDouble(15.48); 	| DOUBLE 	| 15.48 	|
| - 	| - 	| BasicDuration 	| BasicDuration basicDuration = new BasicDuration(Entity.DURATION.SECOND, 1); 	| DURATION 	| 1s, 3M, 5y, 200ms 	|
| Float 	| Float number = Float.valueOf(3.14f) 	| BasicFloat 	| BasicFloat basicFloat = new BasicFloat(2.1f); 	| FLOAT 	| 2.1f 	|
| Integer 	| Integer number = 1; 	| BasicInt 	| BasicInt basicInt = new BasicInt(1); 	| INT 	| 1 	|
| - 	| - 	| BasicInt128 	| BasicInt128 basicInt128 = BasicInt128.fromString("e1671797c52e15f763380b45e841ec32"); 	| INT128 	| e1671797c52e15f763380b45e841ec32 	|
| - 	| - 	| BasicIPAddr 	| BasicIPAddr basicIPAddr = BasicIPAddr.fromString("192.168.1.13"); 	| IPADDR 	| 192.168.1.13 	|
| Long 	| Long number = 123456789L; 	| BasicLong 	| BasicLong basicLong = new BasicLong(367); 	| LONG 	| 367l 	|
| YearMonth 	| YearMonth yearMonth = YearMonth.of(2023, 6); 	| BasicMonth 	| BasicMonth basicMonth = new BasicMonth(YearMonth.of(2022, 7)); 	| MONTH 	| 2012.06M 	|
| LocalTime 	| LocalTime specificTime = LocalTime.of(10, 30, 0);  	| BasicNanoTime 	| BasicNanoTime basicNanoTime = new BasicNanoTime(LocalTime.of(1, 1, 1, 1323433)); 	| NANOTIME 	| 13:30:10.008007006 	|
|   	| same as above 	| BasicSecond 	| BasicSecond basicSecond = new BasicSecond(LocalTime.of(2, 2, 2)); 	| SECOND 	| 13:30:10 	|
|   	| same as above 	| BasicMinute 	| BasicMinute basicMinute = new BasicMinute(new GregorianCalendar()); 	| MINUTE 	| 13:30m 	|
|   	| same as above 	| BasicTime 	| BasicTime basicTime = new BasicTime(LocalTime.of(13, 7, 55)); 	| TIME 	| 13:30:10.008 	|
| - 	| - 	| BasicPoint 	| BasicPoint basicPoint = new BasicPoint(6.4, 9.2); 	| POINT 	| (117.60972, 24.118418) 	|
| short 	| short number = 100;  	| BasicShort 	| BasicShort basicShort = new BasicShort((short) 21); 	| SHORT 	| 122h 	|
| String 	| String s = "abcd"; 	| BasicString 	| BasicString basicString = new BasicString("colDefs"); 	| STRING 	| "Hello" or 'Hello' or `Hello 	|
| - 	| - 	| BasicString 	| BasicString basicString = new BasicString("Jmeter", true); 	| BLOB 	| - 	|
| UUID 	| UUID uuid = UUID.randomUUID(); 	| BasicUuid 	| BasicUuid.fromString("5d212a78-cc48-e3b1-4235-b4d91473ee87") 	| UUID 	| 5d212a78-cc48-e3b1-4235-b4d91473ee87 	|

Data forms

|  	|  	|  	|  	|  	|  	|
|---	|---	|---	|---	|---	|---	|
| Set 	| - 	| BasicSet 	| BasicSet bs = new BasicSet(Entity.DATA_TYPE.DT_INT,4); 	| set 	| x=set(\[5,5,3,4\]);<br>x; 	|
| - 	| - 	| BasicDictionary 	| BasicDictionary bd = new BasicDictionary(Entity.DATA_TYPE.DT_STRING, Entity.DATA_TYPE.DT_DATETIME,2); 	| DICTIONARY 	| x=1 2 3 1;<br>y=2.3 4.6 5.3 6.4;<br>z=dict(x, y); 	|

The majority of DolphinDB data types can be constructed from corresponding Java data types. For examples, INT in DolphinDB from 'new BasicInt(4)', DOUBLE in DolphinDB from 'new BasicDouble(1.23)'. The following DolphinDB data types, however, need to be constructed in different ways:

- CHAR type: as the CHAR type in DolphinDB is stored as a byte, we can use the BasicByte type to construct CHAR in Java API, for example 'new BasicByte((byte)'c')'.
- SYMBOL type: the SYMBOL type in DolphinDB is stored as INT to improve the efficiency of storage and query of STRING vectors (especially for vectors containing multiple duplicate strings). Java API does not provide BasicSymbol object. It uses BasicString to deal with strings. The Java API has provided the BasicSymbolVector type for handling STRING vectors since version 1.30.17.1. Note that when downloading data to Java, it is recommended to use AbstractVector and the `getString` method to access downloaded SYMBOL data, instead of casting to BasicSymbolVector or BasicStringVector.
- Temporal types: temporal data types are stored as INT or LONG in DolphinDB. DolphinDB provides 9 temporal data types: date, month, time, minute, second, datetime, timestamp, nanotime and nanotimestamp. For detailed description, please refer to [DolphinDB Temporal Type and Conversion](https://www.dolphindb.com/help/DataManipulation/TemporalObjects/TemporalTypeandConversion.html). Since Java also provides data types such as LocalDate, LocalTime, LocalDateTime and YearMonth, Java API provides conversion functions in the `Utils` class between all Java temporal types and INT or LONG.

The following script shows the correspondence between DolphinDB temporal types and Java primitive temporal types:

```java
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
BasicNanoTimestamp bnts = new BasicNanoTimestamp(LocalDateTime.of(2018,11,12,8,1,1,123456789));
```

If a temporal variable is stored as timestamp in a third-party system, DolphinDB temporal object can also be instantiated with a timestamp. The `Utils` class in the Java API provides conversion algorithms for various temporal types and standard timestamps, such as converting millisecond timestamps to DolphinDB's BasicTimestamp objects:

```
LocalDateTime dt = Utils.parseTimestamp(1543494854000l);
BasicTimestamp ts = new BasicTimestamp(dt);
```

We can also convert a DolphinDB object to a timestamp of an integer or long integer. For examples:

```
LocalDateTime dt = ts.getTimestamp();
long timestamp = Utils.countMilliseconds(dt);
```

The `Utils` class provides the following methods to handle a variety of timestamp precisions:

- Utils.countMonths: calculate the monthly difference between a given time and 1970.01, returning INT.
- Utils.countDays: calculate the difference in the number of days between the given time and 1970.01.01, returning INT.
- Utils.countMinutes: calculate the minute difference between the given time and 1970.01.01T00:00, returning INT.
- Utils.countSeconds: calculate the difference in seconds between a given time and 1970.01.01T00:00:00, returning INT.
- Utils.countMilliseconds: calculate the difference in milliseconds between a given time and 1970.01.01T00:00:00, returning LONG.
- Utils.countNanoseconds: calculate the difference in nanoseconds between a given time and 1970.01.01T00:00:00.000, returning LONG.


## 9. Java Streaming API

A Java program can subscribe to streaming data via API. Java API can acquire streaming data in the following 3 ways: ThreadedClient, ThreadPooledClient, and PollingClient.

### 9.1. Interfaces

The corresponding interfaces of `subscribe` are:

(1) Subscribe using ThreadedClient:

```java
subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect, IVector filter, int batchSize, float throttle = 0.01f, StreamDeserializer deserializer = null, string user = "", string password = "")
```

**Parameters:**

- **host:** the IP address of the publisher node.
- **port:** the port number of the publisher node.
- **tableName:** a string indicating the name of the publishing stream table.
- **actionName:** a string indicating the name of the subscription task.
- **handler:** a user-defined function to process the subscribed data.
- **offset:** an integer indicating the position of the first message where the subscription begins. A message is a row of the stream table. If *offset* is unspecified, negative or exceeding the number of rows in the stream table, the subscription starts with the next new message. *offset* is relative to the first row of the stream table when it is created. If some rows were cleared from memory due to cache size limit, they are still considered in determining where the subscription starts.
- **reconnect:** a Boolean value indicating whether to resubscribe after network disconnection.
- **filter:** a vector indicating the filtering conditions. Only the rows with values of the filtering column in the vector specified by the parameter *filter* are published to the subscriber.
- **batchSize:** an integer indicating the number of unprocessed messages to trigger the *handler*. If it is positive, the *handler* does not process messages until the number of unprocessed messages reaches *batchSize*. If it is unspecified or non-positive, the *handler* processes incoming messages as soon as they come in.
- **throttle:** an integer indicating the maximum waiting time (in seconds) before the *handler* processes the incoming messages. The default value is 1. This optional parameter has no effect if *batchSize* is not specified.
- **deserializer:** the deserializer for the subscribed heterogeneous stream table.
- **user:** a string indicating the username used to connect to the server
- **password:** a string indicating the password used to connect to the server

(2) Subscribe using ThreadPooledClient:

```java
subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect, IVector filter, StreamDeserializer deserializer = null, string user = "", string password = "")
```

(3) Subscribe using PollingClient:

```java
subscribe(string host, int port, string tableName, string actionName, long offset, bool reconnect, IVector filter, StreamDeserializer deserializer = null, string user = "", string password = "")
```

### 9.2. Code Examples

The following example introduces how to subscribe to stream table:

- The application on the client periodically checks if new data has been added to the streaming table. If yes, the application will acquire and consume the new data.

```java
PollingClient client = new PollingClient(subscribePort);
TopicPoller poller1 = client.subscribe(serverIP, serverPort, tableName, offset);

while (true) {
   ArrayList<IMessage> msgs = poller1.poll(1000);
   if (msgs.size() > 0) {
         BasicInt value = msgs.get(0).getEntity(2);  //access the third cell of the first row
   }
} 
```

After poller1 detects that new data is added to the streaming table, it will pull the new data. When there is no new data, the Java program is waiting at poller1.poll method.

- The API uses MessageHandler to get new data

First we need to define the message handler, which needs to implement com.xxdb.streaming.client.MessageHandle interface.

```java
public class MyHandler implements MessageHandler {
       public void doEvent(IMessage msg) {
               BasicInt qty = msg.getValue(2);
               //..data processing
       }
}
```

You can pass the handler instance into function `subscribe` as a parameter with single-thread or multi-thread callbacks.

(1) ThreadedClient

```java
ThreadedClient client = new ThreadedClient(subscribePort);
client.subscribe(serverIP, serverPort, tableName, new MyHandler(), offsetInt);
```

When new data is added to the streaming table, the system notifies Java API to use 'MyHandler' method to acquire the new data.

(2) ThreadPooledClient

```java
ThreadPooledClient client = new ThreadPooledClient(10000,10);
client.subscribe(serverIP, serverPort, tableName, new MyHandler(), offsetInt);
```

### 9.3. Reconnect

Parameter *reconnect* is a Boolean value indicating whether to automatically resubscribe after the subscription experiences an unexpected interruption. The default value is false.

If *reconnect*=true, whether and how the system resumes the subscription depends on how the unexpected interruption of subscription is caused.

- If the publisher and the subscriber both stay on but the network connection is interrupted, then after network is restored, the subscriber resumes subscription from where the network interruption occurs.
- If the publisher crashes, the subscriber will keep attempting to resume subscription after the publisher restarts.
  - If persistence was enabled on the publisher, the publisher starts to read the persisted data on disk after restarting. The subscriber can't successfully resubscribe automatically until the publisher has read the data for the time when the publisher crashed.
  - If persistence was not enabled on the publisher, the subscriber will fail to automatically resubscribe.
- If the subscriber crashes, the subscriber won't automatically resume the subscription after it restarts. In this case, we need to execute function `subscribe` again.

Parameter 'reconnect' is set to be true for the following example：

```java
PollingClient client = new PollingClient(subscribePort);
TopicPoller poller1 = client.subscribe(serverIP, serverPort, tableName, offset, true);
```

### 9.4. Filter

Parameter *filter* is a vector. It is used together with function `setStreamTableFilterColumn` at the publisher node. Function `setStreamTableFilterColumn` specifies the filtering column in the streaming table. Only the rows with filtering column values in *filter* are published.

In the following example, parameter *filter* is assigned an INT vector \[1,2\]:

```java
BasicIntVector filter = new BasicIntVector(2);
filter.setInt(0, 1);
filter.setInt(1, 2);

PollingClient client = new PollingClient(subscribePort);
TopicPoller poller1 = client.subscribe(serverIP, serverPort, tableName, actionName, offset, filter);
```

### 9.5. Subscribe to a Heterogeneous Table

Since DolphinDB server version 1.30.17/2.00.5, the [replay](https://dolphindb.com/help/FunctionsandCommands/FunctionReferences/r/replay.html) function supports replaying (serializing) multiple stream tables with different schemata into a single stream table (known as "heterogeneous stream table"). Starting from DolphinDB Java API version 1.30.19, a new class `streamDeserializer` has been introduced for the subscription and deserialization of heterogeneous stream table.

#### 9.5.1. Construct Deserializer for Heterogeneous Stream Table

You can construct a deserializer for heterogeneous table with `streamDeserializer`.

**(1) With specified table schema:**

- specified schema

```java
StreamDeserializer(Map<String, BasicDictionary> filters)
```

- specified column types

```java
StreamDeserializer(HashMap<String, List<Entity.DATA_TYPE>> filters)
```

**(2) With specified table:**

```java
StreamDeserializer(Map<String, Pair<String, String>> tableNames, DBConnection conn)
```

Code example:

```java
//Supposing the inputTables to be replayed is:
//d = dict(['msg1', 'msg2'], [table1, table2]); \
//replay(inputTables = d, outputTables = `outTables, dateColumn = `timestampv, timeColumn = `timestampv)";

//create a deserializer for heterogeneous table 

{//specify schema
    BasicDictionary table1_schema = (BasicDictionary)conn.run("table1.schema()");
    BasicDictionary table2_schema = (BasicDictionary)conn.run("table2.schema()");
    Map<String,BasicDictionary > tables = new HashMap<>();
    tables.put("msg1", table1_schema);
    tables.put("msg2", table2_schema);
    StreamDeserializer streamFilter = new StreamDeserializer(tables);
}
{//or specify column types
    Entity.DATA_TYPE[] array1 = {DT_DATETIME,DT_TIMESTAMP,DT_SYMBOL,DT_DOUBLE,DT_DOUBLE};
    Entity.DATA_TYPE[] array2 = {DT_DATETIME,DT_TIMESTAMP,DT_SYMBOL,DT_DOUBLE};
    List<Entity.DATA_TYPE> filter1 = new ArrayList<>(Arrays.asList(array1));
    List<Entity.DATA_TYPE> filter2 = new ArrayList<>(Arrays.asList(array2));
    HashMap<String, List<Entity.DATA_TYPE>> filter = new HashMap<>();
    filter.put("msg1",filter1);
    filter.put("msg2",filter2);
    StreamDeserializer streamFilter = new StreamDeserializer(filter);
}
{//specify tables
    Map<String, Pair<String, String>> tables = new HashMap<>();
    tables.put("msg1", new Pair<>("", "table1"));
    tables.put("msg2", new Pair<>("", "table2"));
    //conn is an optional parameter
    StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
}
```

#### 9.5.2. Subscribe to a Heterogeneous Table

(1) subscribe to a heterogeneous table using ThreadedClient:

- you can specify the parameter *deserializer* of function `subscribe` so as to deserialize the table when data is ingested:

```java
ThreadedClient client = new ThreadedClient(8676);
client.subscribe(hostName, port, tableName, actionName, handler, 0, true, null, streamFilter, false);
```

- you can also add the streamFilter to user-defined Handler:

```java
class Handler6 implements MessageHandler {
    private StreamDeserializer deserializer_;
    private List<BasicMessage> msg1 = new ArrayList<>();
    private List<BasicMessage> msg2 = new ArrayList<>();

    public Handler6(StreamDeserializer deserializer) {
        deserializer_ = deserializer;
    }
    public void batchHandler(List<IMessage> msgs) {
    }

    public void doEvent(IMessage msg) {
        try {
                BasicMessage message = deserializer_.parse(msg);
                if (message.getSym().equals("msg1")) {
                    msg1.add(message);
                } else if (message.getSym().equals("msg2")) {
                    msg2.add(message);
                }
        } catch (Exception e) {
                e.printStackTrace();
        }
    }

    public List<BasicMessage> getMsg1() {
        return msg1;
    }
    public List<BasicMessage> getMsg2() {
        return msg2;
    }
}

ThreadedClient client = new ThreadedClient(listenport);
Handler6 handler = new Handler6(streamFilter);
client.subscribe(hostName, port, tableName, actionName, handler, 0, true);
```

(2) subscribing to a heterogeneous table using ThreadPooledClient is similar as above:

- specify the parameter *deserializer* of function `subscribe`

```java
Handler6 handler = new Handler6(streamFilter);
ThreadPooledClient client1 = new ThreadPooledClient(listenport, threadCount);
client.subscribe(hostName, port, tableName, actionName, handler, 0, true);
```

- add the streamFilter to user-defined Handler:

```java
ThreadPooledClient client = new ThreadPooledClient(listenport, threadCount);
client.subscribe(hostName, port, tableName, actionName, handler, 0, true, null, streamFilter, false);
```

(3) As PollingClient does not support callbacks, you can only pass the *deserializer* parameter to the function `subscribe`:

```java
PollingClient client = new PollingClient(listenport);
TopicPoller poller = subscribe(hostName, port, tableName, actionName, 0, true, null, streamFilter);
```

### 9.6. Unsubscribe

Each subscription is identified with a subscription topic. Subscription fails if a topic with the same name already exists. You can cancel the subscription with `unsubscribe`.

```java
client.unsubscribe(serverIP, serverPort, tableName,actionName);
```

 
