本教程主要介绍以下内容：
- [1. Java API 概述](#1-java-api-概述)
- [2. 建立DolphinDB连接](#2-建立dolphindb连接)
  - [2.1 DBConnection](#21-dbconnection)
  - [2.2 ExclusiveDBConnectionPool](#22-exclusivedbconnectionpool)
- [3.运行DolphinDB脚本](#3运行dolphindb脚本)
- [4. 运行DolphinDB函数](#4-运行dolphindb函数)
- [5. 上传本地对象到DolphinDB服务器](#5-上传本地对象到dolphindb服务器)
- [6. 读取数据示例](#6-读取数据示例)
- [7. 读写DolphinDB数据表](#7-读写dolphindb数据表)
  - [7.1 保存数据到DolphinDB内存表](#71-保存数据到dolphindb内存表)
    - [7.1.1 使用 `insert into` 保存单条数据](#711-使用-insert-into-保存单条数据)
    - [7.1.3 使用`tableInsert`函数保存BasicTable对象](#713-使用tableinsert函数保存basictable对象)
  - [7.2 保存数据到本地磁盘表](#72-保存数据到本地磁盘表)
  - [7.3 保存数据到分布式表](#73-保存数据到分布式表)
  - [7.4 读取和使用数据表](#74-读取和使用数据表)
  - [7.5 批量异步追加数据](#75-批量异步追加数据)
- [8. Java原生类型转换为DolphinDB数据类型](#8-java原生类型转换为dolphindb数据类型)
- [9. Java流数据API](#9-java流数据api)
  - [9.1 接口说明](#91-接口说明)
  - [9.2 示例代码](#92-示例代码)
  - [9.3 断线重连](#93-断线重连)
  - [9.4 启用filter](#94-启用filter)
  - [9.5 订阅异构流表](#95-订阅异构流表)
  - [9.6 取消订阅](#96-取消订阅)

## 1. Java API 概述

Java API需要运行在Java 1.8或以上环境。使用Java API前，引入以下maven 依赖（以1.0.27版本为例）
```java
<dependency>
    <groupId>com.dolphindb</groupId>
    <artifactId>api-java</artifactId>
    <version>1.0.27</version>
</dependency>
```
Java API遵循面向接口编程的原则。Java API使用接口类Entity来表示DolphinDB返回的所有数据类型。在Entity接口类的基础上，根据DolphinDB的数据类型，Java API提供了7种拓展接口，分别是scalar，vector，matrix，set，dictionary，table和chart。这些接口类都包含在com.xxdb.data包中。

拓展的接口类|命名规则|例子
---|---|---
scalar|Basic\<DataType\>|BasicInt, BasicDouble, BasicDate, etc.
vector, matrix|Basic\<DataType\>\<DataForm\>|BasicIntVector, BasicDoubleMatrix, BasicAnyVector, BasicArrayVector, etc.
set, dictionary, table|Basic\<DataForm\>|BasicSet, BasicDictionary, BasicTable.
chart||BasicChart

"Basic"表示基本的数据类型接口，\<DataType\>表示DolphinDB数据类型名称，\<DataForm\>是一个DolphinDB数据形式名称。

DolphinDB Java API提供的最核心的对象是DBConnection。Java应用可以通过它在DolphinDB服务器上执行脚本和函数，并在两者之间双向传递数据。

DBConnection类提供如下主要方法：

| 方法名        | 详情          |
|:------------- |:-------------|
|DBConnection([asynchronousTask, useSSL, compress, usePython])|构造对象|
|connect(host, port, [username, password, initialScript, enableHighAvailability, highAvailabilitySites, reconnect])|将会话连接到DolphinDB服务器|
|login(username,password,enableEncryption)|登陆服务器|
|run(script)|将脚本在DolphinDB服务器运行|
|run(functionName,args)|调用DolphinDB服务器上的函数|
|upload(variableObjectMap)|将本地数据对象上传到DolphinDB服务器|
|isBusy()|判断当前会话是否正忙|
|close()|关闭当前会话|

Java API 的实际用例参见[example目录](https://github.com/dolphindb/api-java/tree/master/example)。

## 2. 建立DolphinDB连接

### 2.1 DBConnection

Java API通过TCP/IP协议连接到DolphinDB服务器。连接正在运行的端口号为8848的本地DolphinDB服务器：

```java
import com.xxdb;
DBConnection conn = new DBConnection();
boolean success = conn.connect("localhost", 8848);
```

声明connection变量的时候，有三个可选参数：useSSL（支持SSL），asynchronousTask（支持一部分）。这两个参数默认值为false。 目前只支持linux, 稳定版>=1.10.17,最新版>=1.20.6。

下面例子是，建立支持SSL而非支持异步的connection，要求数据进行压缩。服务器端应该添加参数enableHTTPS=true(单节点部署，需要添加到dolphindb.cfg;集群部署需要添加到cluster.cfg)。

```java
DBConnection conn = new DBConnection(false, true, true);
```

下面建立不支持SSL，但支持异步的connection。异步情况下，只能执行DolphinDB脚本和函数， 且不再有返回值。该功能适用于异步写入数据。

```java
DBConnection conn = new DBConnection(true, false);
```

输入用户名和密码建立连接：
```java
boolean success = conn.connect("localhost", 8848, "admin", "123456");
```

若未使用用户名及密码连接成功，则脚本在Guest权限下运行。后续运行中若需要提升权限，可以使用 conn.login('admin','123456',true) 登录获取权限。

当需要在应用程序里定义和使用自定义函数时，可以使用 initialScript 参数传入函数定义脚本。这样做的好处是：一、无需每次运行`run`函数的时候重复定义这些函数。二、API提供自动重连机制，断线之后重连时会产生新的会话。如果 initialScript 参数不为空，API会在新的会话中自动执行初始化脚本重新注册函数。在一些网络不是很稳定但是应用程序需要持续运行的场景里，这个参数会非常有用。
```java
boolean success = conn.connect("localhost", 8848, "admin", "123456", "");
```
### 2.2 ExclusiveDBConnectionPool
ExclusiveDBConnectionPool可以复用多个DBConnection。可以直接使用ExclusiveDBConnectionPool.run执行命令，也可以通过execute方法执行任务，然后使用BasicDBTask的getResults方法获取该任务的执行结果。
| 方法名        | 详情          |
|:------------- |:-------------|
|ExclusiveDBConnectionPoolExclusiveDBConnectionPool(string host, int port, string uid,string pwd, int count, bool loadBalance,bool enableHighAvailability, string[] highAvailabilitySites = null, string initialScript, bool compress = false, bool useSSL = false, bool usePython = false)|构造函数，参数count为连接数，loadBalance为true会连接不同的节点|
|run(script)|将脚本在DolphinDB服务器运行|
|run(functionName,args)|调用DolphinDB服务器上的函数|
|execute(IDBTask task)|执行任务|
|execute(List<IDBTask> tasks)|执行批量任务|
|getConnectionCount()|获取连接数|
|shutdown|关闭连接池|

BasicDBTask包装了需要执行的脚本和参数。
| 方法名        | 详情          |
|:------------- |:-------------|
|BasicDBTask(string script, List<IEntity> args)|script为需要执行的函数，args为参数。|
|BasicDBTask(string script)|需要执行的脚本|
|isSuccessful|任务是否执行成功|
|getResult|获取脚本运行结果|
|getErrorMsg|获取任务运行时发生的异常信息|

建立一个DBConnection连接数为10的连接池。

```java


```

创建一个任务。

```java

```

检查任务是否执行成功。如果执行成功，获取相应结果；如果失败，获取异常信息。
```java

```

输出
```

```

创建多个任务，在ExclusiveDBConnectionPool上并行调用。

```java

```

检查任务是否都执行成功。如果执行成功，获取相应结果；如果失败，获取异常信息。

```java

```

输出

```java

```

## 3.运行DolphinDB脚本

在Java中运行DolphinDB脚本的语法如下：
```java
conn.run("script");
```

## 4. 运行DolphinDB函数

除了运行脚本之外，run命令还可以直接在远程DolphinDB服务器上执行DolphinDB内置或用户自定义函数。若`run`方法只有一个参数，则该参数为脚本；若`run`方法有两个参数，则第一个参数为DolphinDB中的函数名，第二个参数是该函数的参数。

下面的示例展示Java程序调用DolphinDB内置的`add`函数。`add`函数有两个参数x和y。参数的所在位置不同，也会导致调用方式的不同。可能有以下三种情况：

* 所有参数都在DolphinDB server端

若变量x和y已经通过Java程序在服务器端生成，
```java
conn.run("x = [1,3,5];y = [2,4,6]")
```
那么在Java端要对这两个向量做加法运算，只需直接使用run("script")即可。
```java
public void testFunction() throws IOException{
    Vector result = (Vector)conn.run("add(x,y)");
    System.out.println(result.getString());
}
```

* 仅有一个参数在DolphinDB server端

若变量x已经通过Java程序在服务器端生成，
```java
conn.run("x = [1,3,5]")
```
而参数y要在Java客户端生成，这时就需要使用“部分应用”方式，把参数x固化在`add`函数内。具体请参考[部分应用文档](https://www.dolphindb.cn/cn/help/Functionalprogramming/PartialApplication.html)。

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
* 两个参数都待由Java客户端赋值
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

## 5. 上传本地对象到DolphinDB服务器

可使用`upload`方法，将本地的数据上传到DolphinDB服务器并分配给一个变量。变量名称可以使用三种类型的字符：字母，数字或下划线，第一个字符必须是字母。

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

## 6. 读取数据示例

下面介绍通过DBConnection对象，读取DolphinDB不同类型的数据。

需要导入DolphinDB数据类型包：

```java
import com.xxdb.data.*;
```

- 向量

首先用字符串向量为例，解释Java使用哪种数据类型来接收DolphinDB返回的向量。以下DolphinDB语句返回Java对象BasicStringVector。

```java
rand(`IBM`MSFT`GOOG`BIDU,10)
```
可使用`rows`方法获取向量的长度；可使用`getString`方法按照索引访问向量元素。

```java
public void testStringVector() throws IOException{
    BasicStringVector vector = (BasicStringVector)conn.run("rand(`IBM`MSFT`GOOG`BIDU, 10)");
    int size = vector.rows();
    System.out.println("size: "+size);
    for(int i=0; i<size; ++i)
        System.out.println(vector.getString(i));
}
```

用类似的方式，也可以处理INT, DOUBLE, FLOAT以及其它数据类型的向量或者元组。
```java
public void testDoubleVector() throws IOException{
    BasicDoubleVector vector = (BasicDoubleVector)conn.run("rand(10.0, 10)");
    int size = vector.rows();
    System.out.println("size: "+size);
    for(int i=0; i<size; ++i)
       System.out.println(vector.getDouble(i));
}
```
以下代码获取[`GS, 2, [1,3,5],[0.9, [0.8]]]此元组的第3个元素的数据形式，数据类型以及内容：
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

数组向量（array vector）是 DolphinDB 一种特殊的数据形式。与常规的向量不同，它的每个元素是一个数组，具有相同的数据类型，但长度可以不同。目前支持的数据类型为 Logical, Integral（不包括 INT128, COMPRESS 类型）, Floating, Temporal。  
以下代码展示创建数组向量的两种方式：

```java
//1. 创建 ArrayVector 的两种方式
//通过传入 List<Vector> 创建
List<Vector> value = new ArrayList<>();
int[] intValue1 = new int[]{1,2,3};
int[] intValue2 = new int[]{4,5,6};
value.add(new BasicIntVector(intValue1));
value.add(new BasicIntVector(intValue2));
BasicArrayVector arrayVector1 = new BasicArrayVector(value);
System.out.println(arrayVector1.getString());
//2. 通过设置 index 并传入 Vector 创建
int[] indexes = new int[]{2,4,8};
BasicIntVector vectorValue = new BasicIntVector(new int[]{1,2,3,4,5,6,7,8});
BasicArrayVector arrayVector2 = new BasicArrayVector(indexes, vectorValue);
System.out.println(arrayVector2.getString());
```
结果是[[1,2,3],[4,5,6]]，[[1,2],[3,4],[5,6,7,...]]

使用 getVectorValue 方法获取第二个元素：

```java
Vector v = arrayVector2.getVectorValue(1);
System.out.println(v.getString());
```

结果为一个 Int 类型的向量[3,4]

- 集合

```java
public void testSet() throws IOException{
	BasicSet result = (BasicSet)conn.run("set(1..100)");
	System.out.println(result.rows()==100);
	System.out.println(((BasicInt)result.keys().get(0)).getInt()==1);
}
```

- 矩阵

要从整数矩阵中检索一个元素，可以使用`getInt`。 要获取行数和列数，可以使用函数`rows`和`columns`。

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

- 字典

用函数`keys`和`values`可以从字典取得所有的键和值。要获得一个键对应的值，可以调用`get`。

```java
public void testDictionary() throws IOException{
		BasicDictionary dict = (BasicDictionary)conn.run("dict(1 2 3,`IBM`MSFT`GOOG)");
        System.out.println(dict.keys());  //[1, 2, 3]
		System.out.println(dict.values()); //[IBM, MSFT, GOOG]
		//to print the corresponding value for key 1.
		System.out.println(dict.get(new BasicInt(1)).getString()); //IBM
}
```

- 表

要获取一个表中某列，可以用table.getColumn(index)。使用table.columns()和table.rows()来分别获取一个表的列数和行数。

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
- NULL对象

要判断一个对象是否为NULL，我们可以使用`obj.getDataType()`。
```java
public void testVoid() throws IOException{
	Entity obj = conn.run("NULL");
	System.out.println(obj.getDataType().equals(Entity.DATA_TYPE.DT_VOID)); //true
}
```

## 7. 读写DolphinDB数据表

DolphinDB数据表按存储方式分为三种:

- 内存表: 数据仅保存在内存中，存取速度最快，但是节点关闭后数据就不存在了。
- 本地磁盘表：数据保存在本地磁盘上。可以从磁盘加载到内存。
- 分布式表：数据分布在不同的节点，通过DolphinDB的分布式计算引擎，逻辑上仍然可以像本地表一样做统一查询。

### 7.1 保存数据到DolphinDB内存表

DolphinDB提供多种方式来保存数据到内存表：
- 通过`insert into`保存单条数据
- 通过`tableInsert`函数批量保存多条数据
- 通过`tableInsert`函数保存数据表

一般不建议通过`append!`函数保存数据，因为`append!`函数会返回表的schema，产生不必要的通信量。

下面分别介绍三种方式保存数据的实例，在例子中使用到的数据表有4个列，分别是string, int, timestamp, double类型，列名分别为cstring, cint, ctimestamp, cdouble。
```java
t = table(10000:0,`cstring`cint`ctimestamp`cdouble,[STRING,INT,TIMESTAMP,DOUBLE])
share t as sharedTable
```
由于内存表是会话隔离的，所以该内存表只有当前会话可见。如果需要在其它会话中访问，需要通过`share`在会话间共享内存表。

#### 7.1.1 使用 `insert into` 保存单条数据

若将单条数据记录保存到DolphinDB内存表，可以使用类似SQL语句insert into。
```java
public void test_save_Insert(String str,int i, long ts,double dbl) throws IOException{
    conn.run(String.format("insert into sharedTable values('%s',%s,%s,%s)",str,i,ts,dbl));
}
```

#### 7.1.2 使用`tableInsert`函数批量保存数组对象 <!-- omit in toc -->

`tableInsert`函数比较适合用来批量保存数据，它可将多个数组追加到DolphinDB内存表中。若Java程序获取的数据可以组织成List方式，可使用`tableInsert`函数保存。

```java
public void test_save_TableInsert(List<String> strArray,List<Integer> intArray,List<Long> tsArray,List<Double> dblArray) throws IOException{
    //用数组构造参数
    List<Entity> args = Arrays.asList(new BasicStringVector(strArray),new BasicIntVector(intArray),new BasicTimestampVector(tsArray),new BasicDoubleVector(dblArray));
    conn.run("tableInsert{sharedTable}", args);
}
```
在本例中，使用了DolphinDB 中的“部分应用”这一特性，将服务端表名以tableInsert{sharedTable}的方式固化到`tableInsert`中，作为一个独立函数来使用。具体文档请参考[部分应用文档](https://www.dolphindb.cn/cn/help/Functionalprogramming/PartialApplication.html)。

#### 7.1.3 使用`tableInsert`函数保存BasicTable对象

若Java程序获取的数据处理后组织成BasicTable对象，`tableInsert`函数也可以接受一个表对象作为参数，批量添加数据。

```java
public void test_save_table(BasicTable table1) throws IOException {
    List<Entity> args = Arrays.asList(table1);
    conn.run("tableInsert{shareTable}", args);
}
```

### 7.2 保存数据到本地磁盘表

通常本地磁盘表用于学习环境或者单机静态数据集测试，它不支持事务，不持支并发读写，不保证运行中的数据一致性，所以不建议在生产环境中使用。

使用DolphinDB脚本创建一个数据表：
```java
dbPath = "C:/data/testDatabase"
tbName = 'tb1'

if(existsDatabase(dbPath)){dropDatabase(dbPath)}
db = database(dbPath,RANGE,2018.01.01..2018.12.31)
db.createPartitionedTable(t,tbName,'ctimestamp')
```
DolphinDB提供`loadTable`方法同样可以加载本地磁盘表，通过`tableInsert`追加数据。
```java
public void test_save_table(String dbPath, BasicTable table1) throws IOException{
    List<Entity> args = new ArrayList<Entity>(1);
    args.add(table1);
    conn.run(String.format("tableInsert{loadTable('%s','tb1')}",dbPath), args);
}
```

### 7.3 保存数据到分布式表

分布式表是DolphinDB推荐在生产环境下使用的数据存储方式，它支持快照级别的事务隔离，保证数据一致性。分布式表支持多副本机制，既提供了数据容错能力，又能作为数据访问的负载均衡。

#### 7.3.1 使用`tableInsert`函数保存BasicTable对象 <!-- omit in toc -->

```java
dbPath = 'dfs://testDatabase'
tbName = 'tb1'

if(existsDatabase(dbPath)){dropDatabase(dbPath)}
db = database(dbPath,RANGE,2018.01.01..2018.12.31)
db.createPartitionedTable(t,tbName,'ctimestamp')
```

DolphinDB提供`loadTable`方法可以加载分布式表，通过`tableInsert`方式追加数据，具体的脚本示例如下：

```java
public void test_save_table(String dbPath, BasicTable table1) throws IOException{
    List<Entity> args = new ArrayList<Entity>(1);
    args.add(table1);
    conn.run(String.format("tableInsert{loadTable('%s','tb1')}",dbPath), args);
}
```

Java程序中的数组或列表，也可以很方便的构造出BasicTable用于追加数据。例如若有 boolArray, intArray, dblArray, dateArray, strArray 这5个列表对象(List\<T\>),可以通过以下语句构造BasicTable对象：

```java
List<String> colNames =  Arrays.asList("cbool","cint","cdouble","cdate","cstring");
List<Vector> cols = Arrays.asList(new BasicBooleanVector(boolArray),new BasicIntVector(intArray),new BasicDoubleVector(dblArray),new BasicDateVector(dateArray),new BasicStringVector(strArray));
BasicTable table1 = new BasicTable(colNames,cols);
```

#### 7.3.2 分布式表的并发写入 <!-- omit in toc -->

DolphinDB的分布式表支持并发读写，下面展示如何在Java客户端中将数据并发写入DolphinDB的分布式表。

> 请注意：DolphinDB不允许多个writer同时将数据写入到同一个分区，因此在客户端多线程并行写入数据时，需要确保每个线程分别写入不同的分区。Java API提供了自动按分区分流数据并行写入的简便方法，函数定义如下

```java
public PartitionedTableAppender(String dbUrl, String tableName, String partitionColName, String appendFunction, DBConnectionPool pool)
```
* dbUrl: 必填，分布式数据库地址
* tableName: 必填，分布式表名
* partitionColName: 必填，分区字段
* appendFunction: 可选，自定义写入函数名，不填此参数则调用内置tableInsert函数。
* pool: 连接池，并行写入数据。

```java
DBConnectionPool pool = new ExclusiveDBConnectionPool(HOST, PORT, "admin", "123456", 3, true, true);
PartitionedTableAppender appender = new PartitionedTableAppender(dbUrl, tableName , "sym", pool);
```

首先，在DolphinDB服务端执行以下脚本，创建分布式数据库"dfs://DolphinDBUUID"和分布式表"device_status"。其中，数据库按照VALUE-HASH-HASH的组合进行三级分区。

```java
t = table(timestamp(1..10)  as date,string(1..10) as sym)
db1=database(\"\",HASH,[DATETIME,10])
db2=database(\"\",HASH,[STRING,5])
if(existsDatabase(\"dfs://demohash\")){
    dropDatabase(\"dfs://demohash\")
}
db =database(\"dfs://demohash\",COMPO,[db2,db1])
pt = db.createPartitionedTable(t,`pt,`sym`date)
```

> 请注意：DolphinDB不允许多个writer同时将数据写入到同一个分区，因此在客户端多线程并行写入数据时，需要确保每个线程分别写入不同的分区。

使用1.30版本以上的server，可以使用java api中的 PartitionedTableAppender类来写入分布式表，其基本原理是设计一个连接池用于多线程写入，然后利用server的schema函数获取分布式表的分区信息，按指定的分区列将用户写入的数据进行分类分别交给不同的连接来并行写入。
使用示例脚本如下：
```java 
DBConnectionPool conn = new ExclusiveDBConnectionPool(host, Integer.parseInt(port), "admin", "123456", Integer.parseInt(threadCount), false, false);

PartitionedTableAppender appender = new PartitionedTableAppender(dbPath, tableName, "gid", "saveGridData{'" + dbPath + "','" + tableName + "'}", conn);
BasicTable table1 = createTable();
appender.append(table1);            
```

### 7.4 读取和使用数据表

#### 7.4.1 读取分布式表 <!-- omit in toc -->
* 在Java API中读取分布式表使用如下代码一次性读取数据
```java
String dbPath = "dfs://testDatabase";
String tbName = "tb1";
DBConnection conn = new DBConnection();
conn.connect(SERVER, PORT, USER, PASSWORD);
BasicTable table = (BasicTable)conn.run(String.format("select * from loadTable('%s','%s') where cdate = 2017.05.03",dbPath,tbName));
```
* 对于大数据量的表，API提供了分段读取方法。(此方法仅适用于DolphinDB 1.20.5及其以上版本)

java API提供了 EntityBlockReader 对象，在run方法中使用参数 fetchSize指定分段大小，通过read()方法一段段的读取数据，示例如下：

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
在使用上述分段读取的方法时，若数据未读取完毕，需要放弃后续数据的读取时，必须调用skipAll方法来显示忽略后续数据，否则会导致套接字缓冲区滞留数据，引发后续数据的反序列化失败。
正确使用的示例代码如下：
```java
    EntityBlockReader v = (EntityBlockReader)conn.run("table(1..12486 as id)",(ProgressListener) null,4,4,10000);
    BasicTable data = (BasicTable)v.read();
    v.skipAll();
    BasicTable t1 = (BasicTable)conn.run("table(1..100 as id1)"); //若没有skipAll此段会抛出异常。
```

#### 7.4.2 使用BasicTable对象 <!-- omit in toc -->
在Java API中，数据表保存为BasicTable对象。由于BasicTable是列式存储，所以若要在Java API中读取行数据需要先取出需要的列，再取出行。

以下例子中参数BasicTable的有4个列，列名分别为cstring, cint, ctimestamp, cdouble，数据类型分别是STRING, INT, TIMESTAMP, DOUBLE。

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

### 7.5 批量异步追加数据

DolphinDB Java API 提供 `MultithreadedTableWriter` 类对象用于批量异步追加数据，并在客户端维护了一个数据缓冲队列。当服务器端忙于网络 I/O 时，客户端写线程仍然可以将数据持续写入缓冲队列（该队列由客户端维护）。写入队列后即可返回，从而避免了写线程的忙等。目前，`MultithreadedTableWriter` 支持批量写入数据到内存表、分区表和维度表。

注意对于异步写入：

* API 客户端提交任务到缓冲队列，缓冲队列接到任务后，客户端即认为任务已完成。
* 提供 `getStatus` 等接口查看状态。

#### 7.5.1 MultithreadedTableWriter <!-- omit in toc -->
MultithreadedTableWriter支持多线程的并发写入。

MultithreadedTableWriter对象的主要方法介绍如下：

```java
MultithreadedTableWriter(String hostName, int port, String userId, String password,
        String dbName, String tableName, boolean useSSL,
        boolean enableHighAvailability, String[] highAvailabilitySites,
        int batchSize, float throttle,
        int threadCount, String partitionCol,
        int[] compressTypes)
```

参数说明：
* hostName 字符串，表示所连接的服务器的地址.
* port 整数，表示服务器端口。
* userId / password: 字符串，登录时的用户名和密码。
* dbPath 字符串，表示分布式数据库地址。内存表时该参数为空。请注意，1.30.17及以下版本 API，向内存表写入数据时，该参数需填写内存表表名。
* tableName 字符串，表示分布式表或内存表的表名。请注意，1.30.17及以下版本 API，向内存表写入数据时，该参数需为空。
* useSSL 布尔值，表示是否启用加密通讯。
* enableHighAvailability 布尔值，表示是否开启 API 高可用。
* highAvailabilitySites 数组类型，表示所有可用节点的 ip:port 构成的 String数组。
* batchSize 整数，表示批处理的消息的数量。如果该参数值为 1，表示客户端写入数据后就立即发送给服务器；
  如果该参数大于 1，表示数据量达到 batchSize 时，客户端才会将数据发送给服务器。
* throttle 大于0的浮点数，单位为秒。若客户端有数据写入，但数据量不足 batchSize，则等待 throttle的时间再发送数据。
* threadCount 整数，表示创建的工作线程数量，如果值为 1，表示单线程。对于维度表，其值必须为 1。
* partitionCol 字符串类型，默认为空，仅在 threadCount 大于1时起效。对于分区表，必须指定为分区字段名；
  如果是流表，必须指定为表的字段名；对于维度表，该参数不起效。
* compressTypes 数组类型，用于指定每一列采用的压缩传输方式，为空表示不压缩。每一列可选的压缩方式（大小写不敏感）
  包括：
   * Vector.COMPRESS_LZ4：LZ4压缩
   * Vector.COMPRESS_DELTA：DELTAOFDELTA 压缩

以下是MultithreadedTableWriter对象包含的函数方法介绍：

```java
ErrorCodeInfo insert(Object... args)
```

函数说明：

插入单行数据。返回一个 ErrorCodeInfo 类，包含 errorCode 和 errorInfo，分别表示错误代码和错误信息。
当 errorCode 不为 "" 时，表示 MTW 写入失败，此时，errorInfo 会显示失败的详细信息。 之后的版本
中会对错误信息进行详细说明，给出错误信息的代码、错误原因及解决办法。 另外，ErrorCodeInfo 类提供了 
hasError() 和 succeed() 方法用于获取数据插入的结果。hasError() 返回 True，则表示存在错误，否则表示无错误。succeed() 返回 True，则表示插入成功，
否则表示插入失败。

参数说明：

* args: 是变长参数，代表插入一行数据

示例：

```java
ErrorCodeInfo pErrorInfo = multithreadedTableWriter_.insert(new Date(2022, 3, 23), "AAAAAAAB", random.nextInt() % 10000);
```

```java
List<List<Entity>> getUnwrittenData()
```

函数说明：

返回一个嵌套列表，表示未写入服务器的数据。

注意：该方法获取到数据资源后， MultithreadedTableWriter将释放这些数据资源。

示例：

```java
List<List<Entity>> unwrittenData = multithreadedTableWriter_.getUnwrittenData();
```

```java
ErrorCodeInfo insertUnwrittenData(List<List<Entity>> records)
```

函数说明：

将数据插入数据表。返回值同 insert 方法。与 insert 方法的区别在于，insert 只能插入单行数据，而 insertUnwrittenData 可以同时插入多行数据。

参数说明：

* **records**：需要再次写入的数据。可以通过方法 getUnwrittenData 获取该对象。

示例：

```java
ErrorCodeInfo ret = multithreadedTableWriter_.insertUnwrittenData(unwrittenData);
```

```java
Status getStatus()
```

函数说明：

获取 MultithreadedTableWriter 对象当前的运行状态。

参数说明：

* **status**：是MultithreadedTableWriter.Status 类，具有以下属性和方法

示例：

```java
MultithreadedTableWriter.Status writeStatus = new MultithreadedTableWriter.Status();
writeStatus = multithreadedTableWriter_.getStatus();
```


属性：

* isExiting：写入线程是否正在退出。
* errorCode：错误码。
* errorInfo：错误信息。
* sentRows：成功发送的总记录数。
* unsentRows：待发送的总记录数。
* sendFailedRows：发送失败的总记录数。
* threadStatus：写入线程状态列表。
  - threadId：线程 Id。
  - sentRows：该线程成功发送的记录数。
  - unsentRows：该线程待发送的记录数。
  - sendFailedRows：该线程发送失败的记录数。

方法：

* hasError()：true 表示数据写入存在错误；false 表示数据写入无错误。
* succeed()：true 表示数据写入成功；false 表示数据写入失败。

```java
waitForThreadCompletion()
```

函数说明：

调用此方法后，MTW 会进入等待状态，待后台工作线程全部完成后退出等待状态。

示例：

```java
multithreadedTableWriter_.waitForThreadCompletion();
```

MultithreadedTableWriter 的正常使用示例如下：

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
    //插入100行正确数据
    for (int i = 0; i < 100; ++i)
    {
        ret = multithreadedTableWriter_.insert(new Date(2022, 3, 23), "AAAAAAAB", random.nextInt() % 10000);
    }
} 
catch (Exception e)
{   //MTW 抛出异常
    System.out.println("MTW exit with exception {0}" + e.getMessage());
}

//等待 MTW 插入完成
multithreadedTableWriter_.waitForThreadCompletion();
MultithreadedTableWriter.Status writeStatus = new MultithreadedTableWriter.Status();
writeStatus = multithreadedTableWriter_.getStatus();
if (!writeStatus.errorInfo.equals(""))
{
    //如果写入时发生错误
    System.out.println("error in writing !");
}
System.out.println("writeStatus: {0}\n" + writeStatus.toString());
System.out.println(((BasicLong)conn.run("exec count(*) from pt")).getLong());
```

以上代码输出结果为：
```java
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

调用 writer.insert() 方法向 writer 中写入数据，并通过 writer.getStatus() 获取 writer 的状态。
注意，使用 writer.waitForThreadCompletion() 方法等待 MTW 写入完毕，会终止 MTW 所有工作线程，保留最后一次写入信息。此时如果需要再次将数据写入 MTW，需要重新获取新的 MTW 对象，才能继续写入数据。

由上例可以看出，MTW 内部使用多线程完成数据转换和写入任务。但在 MTW 外部，API 客户端同样支持以多线程方式将数据写入 MTW，且保证了多线程安全。

#### 7.5.2 MultithreadedTableWriter返回异常的几种形式 <!-- omit in toc -->

MultithreadedTableWriter 类调用 insert 方法插入数据时发生异常：

在调用 MultithreadedTableWriter 的 insert 方法时，若插入数据的类型与表对应列的类型不匹配，则 MultithreadedTableWriter 会立刻返回错误信息并打印出堆栈。

示例：

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
//插入1行类型错误数据，MTW 立刻返回错误信息
ret = multithreadedTableWriter_.insert(new Date(2022, 3, 23), 222, random.nextInt() % 10000);
if (!ret.errorInfo.equals(""))
    System.out.println("insert wrong format data: {2}\n" + ret.toString());
```

以上代码输出结果为：

```java
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

在调用 MultithreadedTableWriter 的 insert 方法时，若 insert 插入数据的列数和表的列数不匹配，MultithreadedTableWriter 会立刻返回错误信息。

示例：

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
//插入1行数据，插入数据的列数和表的列数不匹配，MTW 立刻返回错误信息
ret = multithreadedTableWriter_.insert(new Date(2022, 3, 23), random.nextInt() % 10000);
if (!ret.errorInfo.equals(""))
    System.out.println("insert wrong format data: {3}\n" + ret.toString());
```

以上代码输出结果为：

```java
"""
    insert wrong format data: {3}
      code=A2 info=Column counts don't match.  
"""
```

如果 MultithreadedTableWriter 在运行时连接断开，则所有工作线程被终止。继续通过 MultithreadedTableWriter 向服务器写数据时，会因为工作线程终止而抛出异常，且数据不会被写入。此时，
可通过调用 MultithreadedTableWriter 的 getUnwrittenData 获取未插入的数据，并重新插入。

示例：

```java
List<List<Entity>> unwriterdata = new ArrayList<>();
unwriterdata = multithreadedTableWriter_.getUnwrittenData();
System.out.println("{5} unwriterdata: " + unwriterdata.size());
//重新获取新的 MTW 对象
MultithreadedTableWriter newmultithreadedTableWriter = new MultithreadedTableWriter(HOST, PORT, "admin", "123456", "dfs://valuedb3", "pdatetest",
        false, false, null, 10000, 1,
        5, "id", new int[]{Vector.COMPRESS_LZ4, Vector.COMPRESS_LZ4, Vector.COMPRESS_DELTA});
try
{
    boolean writesuccess = true;
    //将没有写入的数据写到新的 MTW 中
    ret = newmultithreadedTableWriter.insertUnwrittenData(unwriterdata);
}
finally
{
    newmultithreadedTableWriter.waitForThreadCompletion();
    writeStatus = newmultithreadedTableWriter.getStatus();
    System.out.println("writeStatus: {6}\n" + writeStatus.toString());
}
```

以上代码输出结果为：

```java
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

## 8. Java原生类型转换为DolphinDB数据类型

Java API提供了一组以Basic+\<DataType\>方式命名的类，分别对应DolphinDB的数据类型，比如BasicInt类，BasicDate类等等。

大部分DolphinDB数据类型可以由对应的Java数据类型构建，例如new BasicInt(4)对应integer，new BasicDouble(1.23)对应double，等等。但是也有一些DolphinDB数据类型，并不能由上述方法构建：

- CHAR类型：DolphinDB中的CHAR类型保存为一个byte，所以在Java API中用BasicByte类型来构造CHAR，例如new BasicByte((byte)'c')。
- SYMBOL类型：DolphinDB中的SYMBOL类型将字符串存储为整形，可以提高对字符串数据存储和查询的效率，但是Java中并没有这种类型，所以Java API不提供BasicSymbol这种对象，直接用BasicString来处理即可。
- 时间类型：DolphinDB的时间类型是以整形或者长整形来描述的，DolphinDB提供date, month, time, minute, second, datetime, timestamp, nanotime和nanotimestamp九种类型的时间类型，最高精度可以到纳秒级。具体的描述可以参考[DolphinDB时序类型和转换](https://www.dolphindb.cn/cn/help/DataManipulation/TemporalObjects/TemporalTypeandConversion.html)。由于Java也提供了LocalDate, LocalTime, LocalDateTime, YearMonth等数据类型，所以Java API在Utils类里提供了所有Java时间类型与int或long之间的转换函数。

以下脚本展示Java API中DolphinDB时间类型与Java原生时间类型之间的对应关系：
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
如果在第三方系统中时间以时间戳的方式存储，DolphinDB时间对象也可以用时间戳来实例化。Java API中的Utils类提供了各种时间类型与标准时间戳的转换算法，比如将毫秒级的时间戳转换为DolphinDB的BasicTimestamp对象:
```java
LocalDateTime dt = Utils.parseTimestamp(1543494854000l);
BasicTimestamp ts = new BasicTimestamp(dt);
```
也可以将DolphinDB对象转换为整型或长整型的时间戳，比如：
```java
LocalDateTime dt = ts.getTimestamp();
long timestamp = Utils.countMilliseconds(dt);
```
如果时间戳以其他精度保存，Utils类还中提供如下方法，可以适应各种不同的精度：
- Utils.countMonths：计算给定时间到1970.01之间的月份差，返回INT.
- Utils.countDays：计算给定时间到1970.01.01之间的天数差，返回INT.
- Utils.countMinutes：计算给定时间到1970.01.01T00:00之间的分钟差，返回INT.
- Utils.countSeconds：计算给定时间到1970.01.01T00:00:00之间的秒数差，返回INT.
- Utils.countMilliseconds：计算给定时间到1970.01.01T00:00:00之间的毫秒数差，返回LONG.
- Utils.countNanoseconds：计算给定时间到1970.01.01T00:00:00.000之间的纳秒数差，返回LONG.

## 9. Java流数据API

Java程序可以通过API订阅流数据。Java API有三种获取流数据的方式：单线程回调（ThreadedClient），多线程回调（ThreadPooledClient）和通过 PollingClient 返回的对象获取消息队列。

### 9.1 接口说明
三种方法对应的 subscribe 接口如下：
1. 通过 ThreadedClient 方式订阅的接口：
```cs
subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect, IVector filter, int batchSize, float throttle = 0.01f, StreamDeserializer deserializer = null, string user = "", string password = "")
```
- **host** 是发布端节点的 IP 地址。
- **port** 是发布端节点的端口号。
- **tableName** 是发布表的名称。
- **actionName** 是订阅任务的名称。
- **handler** 是用户自定义的回调函数，用于处理每次流入的数据。
- **offset** 是整数，表示订阅任务开始后的第一条消息所在的位置。消息是流数据表中的行。如果没有指定 *offset*，或它为负数或超过了流数据表的记录行数，订阅将会从流数据表的当前行开始。*offset* 与流数据表创建时的第一行对应。如果某些行因为内存限制被删除，在决定订阅开始的位置时，这些行仍然考虑在内。
- **reconnect** 是布尔值，表示订阅中断后，是否会自动重订阅。
- **filter** 是一个向量，表示过滤条件。流数据表过滤列在 *filter* 中的数据才会发布到订阅端，不在 *filter* 中的数据不会发布。
- **batchSize** 是一个整数，表示批处理的消息的数量。如果它是正数，直到消息的数量达到 *batchSize* 时，*handler* 才会处理进来的消息。如果它没有指定或者是非正数，消息到达之后，*handler* 就会马上处理消息。
- **throttle** 是一个浮点数，表示 *handler* 处理到达的消息之前等待的时间，以秒为单位。默认值为 1。如果没有指定 *batchSize*，*throttle* 将不会起作用。
- **deserializer** 是订阅的异构流表对应的反序列化器。
- **userName** 是一个字符串，表示 API 所连接服务器的登录用户名。
- **password** 是一个字符串，表示 API 所连接服务器的登录密码。

2. 通过 ThreadPooledClient 方式订阅的接口：

```cs
subscribe(string host, int port, string tableName, string actionName, MessageHandler handler, long offset, bool reconnect, IVector filter, StreamDeserializer deserializer = null, string user = "", string password = "")
```
3. 通过 PollingClient 方式订阅的接口：
```cs
subscribe(string host, int port, string tableName, string actionName, long offset, bool reconnect, IVector filter, StreamDeserializer deserializer = null, string user = "", string password = "")
```
### 9.2 示例代码
下面分别介绍如何通过3种方法订阅流数据。  
- 通过客户机上的应用程序定期去流数据表查询是否有新增数据，推荐使用 PollingClient。

```java
PollingClient client = new PollingClient(subscribePort);
TopicPoller poller1 = client.subscribe(serverIP, serverPort, tableName, offset);

while (true) {
   ArrayList<IMessage> msgs = poller1.poll(1000);
   if (msgs.size() > 0) {
         BasicInt value = msgs.get(0).getEntity(2);  //取数据中第一行第三个字段
   }
}
```

poller1探测到流数据表有新增数据后，会拉取到新数据。无新数据发布时，Java程序会阻塞在poller1.poll方法这里等待。

- 使用 MessageHandler 回调的方式获取新数据。

首先需要调用者定义数据处理器handler。handler需要实现com.xxdb.streaming.client.MessageHandler接口。


```java
public class MyHandler implements MessageHandler {
       public void doEvent(IMessage msg) {
               BasicInt qty = msg.getValue(2);
               //..处理数据
       }
}
```

在启动订阅时，把handler实例作为参数传入订阅函数。包括单线程回调或多线程回调两种方式：
1. 单线程回调 ThreadedClient

```java
ThreadedClient client = new ThreadedClient(subscribePort);
client.subscribe(serverIP, serverPort, tableName, new MyHandler(), offsetInt);
```

当流数据表有新增数据时，系统会通知Java API调用MyHandler方法，将新数据通过msg参数传入。
2. 多线程回调(ThreadPollingClient)：handler 模式客户端(线程池处理任务)
```java

```

### 9.3 断线重连

reconnect参数是一个布尔值，表示订阅意外中断后，是否会自动重新订阅。默认值为false。

若reconnect设置为true时，订阅意外中断后系统是否以及如何自动重新订阅，取决于订阅中断由哪种原因导致：

- 如果发布端与订阅端处于正常状态，但是网络中断，那么订阅端会在网络正常时，自动从中断位置重新订阅。
- 如果发布端崩溃，订阅端会在发布端重启后不断尝试重新订阅。
    - 如果发布端对流数据表启动了持久化，发布端重启后会首先读取硬盘上的数据，直到发布端读取到订阅中断位置的数据，订阅端才能成功重新订阅。
    - 如果发布端没有对流数据表启用持久化，那么订阅端将自动重新订阅失败。
- 如果订阅端崩溃，订阅端重启后不会自动重新订阅，需要重新执行`subscribe`函数。

以下例子在订阅时，设置reconnect为true：

```java
PollingClient client = new PollingClient(subscribePort);
TopicPoller poller1 = client.subscribe(serverIP, serverPort, tableName, offset, true);
```

### 9.4 启用filter

filter参数是一个向量。该参数需要发布端配合`setStreamTableFilterColumn`函数一起使用。使用`setStreamTableFilterColumn`指定流数据表的过滤列，流数据表过滤列在filter中的数据才会发布到订阅端，不在filter中的数据不会发布。

以下例子将一个包含元素1和2的整数类型向量作为`subscribe`的filter参数：

```java
BasicIntVector filter = new BasicIntVector(2);
filter.setInt(0, 1);
filter.setInt(1, 2);

PollingClient client = new PollingClient(subscribePort);
TopicPoller poller1 = client.subscribe(serverIP, serverPort, tableName, actionName, offset, filter);
```
### 9.5 订阅异构流表

DolphinDB server 自 1.30.17 及 2.00.5 版本开始，支持通过 [replay](https://www.dolphindb.cn/cn/help/FunctionsandCommands/FunctionReferences/r/replay.html) 函数将多个结构不同的流数据表，回放（序列化）到一个流表里，这个流表被称为异构流表。Python API 自 1.30.19 版本开始，新增 `StreamDeserializer` 类，用于构造异构流表反序列化器，以实现对异构流表的订阅和反序列化操作。

C# API 通过 `StreamDeserializer` 类来构造异构流表反序列化器，语法如下：
1. 通过指定表的schema进行构造，包含以下两种方式，指定表的schema信息或指定表的各列类型 ：

指定表的schema信息：
```java
StreamDeserializer(Map<String, BasicDictionary> filters)
```
指定表的各列类型：
```java
StreamDeserializer(HashMap<String, List<Entity.DATA_TYPE>> filters)
```
2. 通过指定表进行构造：
```java
StreamDeserializer(Map<String, Pair<String, String>> tableNames, DBConnection conn)
```
订阅示例：
```java
//假设异构流表回放时inputTables如下：
//d = dict(['msg1', 'msg2'], [table1, table2]); \
//replay(inputTables = d, outputTables = `outTables, dateColumn = `timestampv, timeColumn = `timestampv)";
//异构流表解析器的创建方法如下：

{//指定schema的方式
    BasicDictionary table1_schema = (BasicDictionary)conn.run("table1.schema()");
    BasicDictionary table2_schema = (BasicDictionary)conn.run("table2.schema()");
    Map<String,BasicDictionary > tables = new HashMap<>();
    tables.put("msg1", table1_schema);
    tables.put("msg2", table2_schema);
    StreamDeserializer streamFilter = new StreamDeserializer(tables);
}
{//指定表的各列类型
    Entity.DATA_TYPE[] array1 = {DT_DATETIME,DT_TIMESTAMP,DT_SYMBOL,DT_DOUBLE,DT_DOUBLE};
    Entity.DATA_TYPE[] array2 = {DT_DATETIME,DT_TIMESTAMP,DT_SYMBOL,DT_DOUBLE};
    List<Entity.DATA_TYPE> filter1 = new ArrayList<>(Arrays.asList(array1));
    List<Entity.DATA_TYPE> filter2 = new ArrayList<>(Arrays.asList(array2));
    HashMap<String, List<Entity.DATA_TYPE>> filter = new HashMap<>();
    filter.put("msg1",filter1);
    filter.put("msg2",filter2);
    StreamDeserializer streamFilter = new StreamDeserializer(filter);
}
{//指定表的方式
    Map<String, Pair<String, String>> tables = new HashMap<>();
    tables.put("msg1", new Pair<>("", "table1"));
    tables.put("msg2", new Pair<>("", "table2"));
    //conn是可选参数，如果不传入，在订阅的时候会自动使用订阅的conn进行构造
    StreamDeserializer streamFilter = new StreamDeserializer(tables, conn);
}
```
下面分别介绍如何通过 ThreadedClient, ThreadPooledClient 和 PollingClient 三种方式订阅异构流表：
1. 通过 ThreadedClient 订阅异构流表：通过两种方式完成订阅时对异构流表的解析操作。
* 通过指定 `subscribe` 函数的 *deserialize* 参数，实现在订阅时直接解析异构流表：
```java
ThreadedClient client = new ThreadedClient(8676);
client.subscribe(hostName, port, tableName, actionName, handler, 0, true, null, streamFilter, false);
```
* 异构流表（streamFilter）也可以写入客户自定义的 Handler 中，在回调时被解析：
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
2. 通过 ThreadPooledClient 订阅异构流表的方法和 ThreadedClient 一致。
* 指定 `subscribe` 函数的 *deserialize* 参数：
```java
Handler6 handler = new Handler6(streamFilter);
ThreadPooledClient client1 = new ThreadPooledClient(listenport, threadCount);
client.subscribe(hostName, port, tableName, actionName, handler, 0, true);
```
* 异构流表（streamFilter）也可以写入客户自定义的 Handler 中，在回调时被解析：
```java
ThreadPooledClient client = new ThreadPooledClient(listenport, threadCount);
client.subscribe(hostName, port, tableName, actionName, handler, 0, true, null, streamFilter, false);
```
由于 PollingClient 没有回调函数，只能通过为 `subscirbe` 的 *deserialize* 参数传入 streamFilter 的方式进行解析：
```java
PollingClient client = new PollingClient(listenport);
TopicPoller poller = subscribe(hostName, port, tableName, actionName, 0, true, null, streamFilter);
```
### 9.6 取消订阅
每一个订阅都有一个订阅主题topic作为唯一标识。如果订阅时topic已经存在，那么会订阅失败。这时需要通过unsubscribeTable函数取消订阅才能再次订阅。
```java
client.unsubscribe(serverIP, serverPort, tableName,actionName);
```
