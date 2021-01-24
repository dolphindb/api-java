本教程主要介绍以下内容：
- [1. Java API 概述](README_CN.md#1-java-api-%E6%A6%82%E8%BF%B0)
- [2. 建立DolphinDB连接](README_CN.md#2-%E5%BB%BA%E7%AB%8Bdolphindb%E8%BF%9E%E6%8E%A5)
- [3.运行DolphinDB脚本](README_CN.md#3%E8%BF%90%E8%A1%8Cdolphindb%E8%84%9A%E6%9C%AC)
- [4. 运行DolphinDB函数](README_CN.md#4-%E8%BF%90%E8%A1%8Cdolphindb%E5%87%BD%E6%95%B0)
- [5. 上传本地对象到DolphinDB服务器](README_CN.md#5-%E4%B8%8A%E4%BC%A0%E6%9C%AC%E5%9C%B0%E5%AF%B9%E8%B1%A1%E5%88%B0dolphindb%E6%9C%8D%E5%8A%A1%E5%99%A8)
- [6. 读取数据示例](README_CN.md#6-%E8%AF%BB%E5%8F%96%E6%95%B0%E6%8D%AE%E7%A4%BA%E4%BE%8B)
- [7. 读写DolphinDB数据表](README_CN.md#7-%E8%AF%BB%E5%86%99dolphindb%E6%95%B0%E6%8D%AE%E8%A1%A8)
  - [7.1 保存数据到DolphinDB内存表](README_CN.md#71-%E4%BF%9D%E5%AD%98%E6%95%B0%E6%8D%AE%E5%88%B0dolphindb%E5%86%85%E5%AD%98%E8%A1%A8)
    - [7.1.1 使用 insert into 保存单条数据](README_CN.md#711-%E4%BD%BF%E7%94%A8-insert-into-%E4%BF%9D%E5%AD%98%E5%8D%95%E6%9D%A1%E6%95%B0%E6%8D%AE)
    - [7.1.2 使用tableInsert函数批量保存数组对象](README_CN.md#712-%E4%BD%BF%E7%94%A8tableinsert%E5%87%BD%E6%95%B0%E6%89%B9%E9%87%8F%E4%BF%9D%E5%AD%98%E6%95%B0%E7%BB%84%E5%AF%B9%E8%B1%A1)
    - [7.1.3 使用tableInsert函数保存BasicTable对象](README_CN.md#713-%E4%BD%BF%E7%94%A8tableinsert%E5%87%BD%E6%95%B0%E4%BF%9D%E5%AD%98basictable%E5%AF%B9%E8%B1%A1)
  - [7.2 保存数据到本地磁盘表](README_CN.md#72-%E4%BF%9D%E5%AD%98%E6%95%B0%E6%8D%AE%E5%88%B0%E6%9C%AC%E5%9C%B0%E7%A3%81%E7%9B%98%E8%A1%A8)
  - [7.3 保存数据到分布式表](README_CN.md#73-%E4%BF%9D%E5%AD%98%E6%95%B0%E6%8D%AE%E5%88%B0%E5%88%86%E5%B8%83%E5%BC%8F%E8%A1%A8)
    - [7.3.1 使用tableInsert函数保存BasicTable对象](README_CN.md#731-%E4%BD%BF%E7%94%A8tableinsert%E5%87%BD%E6%95%B0%E4%BF%9D%E5%AD%98basictable%E5%AF%B9%E8%B1%A1)
    - [7.3.2 分布式表的并发写入](README_CN.md#732-%E5%88%86%E5%B8%83%E5%BC%8F%E8%A1%A8%E7%9A%84%E5%B9%B6%E5%8F%91%E5%86%99%E5%85%A5) 
  - [7.4 读取和使用数据表](README_CN.md#74-%E8%AF%BB%E5%8F%96%E5%92%8C%E4%BD%BF%E7%94%A8%E6%95%B0%E6%8D%AE%E8%A1%A8)
- [8. Java原生类型转换为DolphinDB数据类型](README_CN.md#8-java%E5%8E%9F%E7%94%9F%E7%B1%BB%E5%9E%8B%E8%BD%AC%E6%8D%A2%E4%B8%BAdolphindb%E6%95%B0%E6%8D%AE%E7%B1%BB%E5%9E%8B)
- [9. Java流数据API](README_CN.md#9-java%E6%B5%81%E6%95%B0%E6%8D%AEapi)

### 1. Java API 概述
Java API需要运行在Java 1.8或以上环境。

Java API遵循面向接口编程的原则。Java API使用接口类Entity来表示DolphinDB返回的所有数据类型。在Entity接口类的基础上，根据DolphinDB的数据类型，Java API提供了7种拓展接口，分别是scalar，vector，matrix，set，dictionary，table和chart。这些接口类都包含在com.xxdb.data包中。

拓展的接口类|命名规则|例子
---|---|---
scalar|Basic\<DataType\>|BasicInt, BasicDouble, BasicDate, etc.
vector, matrix|Basic\<DataType\>\<DataForm\>|BasicIntVector, BasicDoubleMatrix, BasicAnyVector, etc.
set, dictionary, table|Basic\<DataForm\>|BasicSet, BasicDictionary, BasicTable.
chart||BasicChart

"Basic"表示基本的数据类型接口，\<DataType\>表示DolphinDB数据类型名称，\<DataForm\>是一个DolphinDB数据形式名称。

DolphinDB Java API提供的最核心的对象是DBConnection。Java应用可以通过它在DolphinDB服务器上执行脚本和函数，并在两者之间双向传递数据。

DBConnection类提供如下主要方法：

| 方法名        | 详情          |
|:------------- |:-------------|
|connect(host, port, [username, password])|将会话连接到DolphinDB服务器|
|login(username,password,enableEncryption)|登陆服务器|
|run(script)|将脚本在DolphinDB服务器运行|
|run(functionName,args)|调用DolphinDB服务器上的函数|
|upload(variableObjectMap)|将本地数据对象上传到DolphinDB服务器|
|isBusy()|判断当前会话是否正忙|
|close()|关闭当前会话|

Java API 的实际用例参见[example目录](https://github.com/dolphindb/api-java/tree/master/example)。

### 2. 建立DolphinDB连接

Java API通过TCP/IP协议连接到DolphinDB服务器。连接正在运行的端口号为8848的本地DolphinDB服务器：

```
import com.xxdb;
DBConnection conn = new DBConnection();
boolean success = conn.connect("localhost", 8848);
```

输入用户名和密码建立连接：
```
boolean success = conn.connect("localhost", 8848, "admin", "123456");
```

若未使用用户名及密码连接成功，则脚本在Guest权限下运行。后续运行中若需要提升权限，可以使用 conn.login('admin','123456',true) 登录获取权限。

### 3.运行DolphinDB脚本

在Java中运行DolphinDB脚本的语法如下：
```
conn.run("script");
```
具体例子可参照下一节。脚本的最大长度为65,535字节。

### 4. 运行DolphinDB函数

除了运行脚本之外，run命令还可以直接在远程DolphinDB服务器上执行DolphinDB内置或用户自定义函数。若`run`方法只有一个参数，则该参数为脚本；若`run`方法有两个参数，则第一个参数为DolphinDB中的函数名，第二个参数是该函数的参数。

下面的示例展示Java程序调用DolphinDB内置的`add`函数。`add`函数有两个参数x和y。参数的所在位置不同，也会导致调用方式的不同。可能有以下三种情况：

* 所有参数都在DolphinDB server端

若变量x和y已经通过Java程序在服务器端生成，
```
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
```
conn.run("x = [1,3,5]")
```
而参数y要在Java客户端生成，这时就需要使用“部分应用”方式，把参数x固化在`add`函数内。具体请参考[部分应用文档](https://www.dolphindb.com/cn/help/PartialApplication.html)。

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

### 5. 上传本地对象到DolphinDB服务器

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

### 6. 读取数据示例

下面介绍通过DBConnection对象，读取DolphinDB不同类型的数据。

需要导入DolphinDB数据类型包：

```
import com.xxdb.data.*;
```

- 向量

首先用字符串向量为例，解释Java使用哪种数据类型来接收DolphinDB返回的向量。以下DolphinDB语句返回Java对象BasicStringVector。

```
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

### 7. 读写DolphinDB数据表

DolphinDB数据表按存储方式分为三种:

- 内存表: 数据仅保存在内存中，存取速度最快，但是节点关闭后数据就不存在了。
- 本地磁盘表：数据保存在本地磁盘上。可以从磁盘加载到内存。
- 分布式表：数据分布在不同的节点，通过DolphinDB的分布式计算引擎，逻辑上仍然可以像本地表一样做统一查询。

#### 7.1 保存数据到DolphinDB内存表

DolphinDB提供多种方式来保存数据到内存表：
- 通过`insert into`保存单条数据
- 通过`tableInsert`函数批量保存多条数据
- 通过`tableInsert`函数保存数据表

一般不建议通过`append!`函数保存数据，因为`append!`函数会返回表的schema，产生不必要的通信量。

下面分别介绍三种方式保存数据的实例，在例子中使用到的数据表有4个列，分别是string, int, timestamp, double类型，列名分别为cstring, cint, ctimestamp, cdouble。
```
t = table(10000:0,`cstring`cint`ctimestamp`cdouble,[STRING,INT,TIMESTAMP,DOUBLE])
share t as sharedTable
```
由于内存表是会话隔离的，所以该内存表只有当前会话可见。如果需要在其它会话中访问，需要通过`share`在会话间共享内存表。

##### 7.1.1 使用 `insert into` 保存单条数据

若将单条数据记录保存到DolphinDB内存表，可以使用类似SQL语句insert into。
```
public void test_save_Insert(String str,int i, long ts,double dbl) throws IOException{
    conn.run(String.format("insert into sharedTable values('%s',%s,%s,%s)",str,i,ts,dbl));
}
```

##### 7.1.2 使用`tableInsert`函数批量保存数组对象

`tableInsert`函数比较适合用来批量保存数据，它可将多个数组追加到DolphinDB内存表中。若Java程序获取的数据可以组织成List方式，可使用`tableInsert`函数保存。

```
public void test_save_TableInsert(List<String> strArray,List<Integer> intArray,List<Long> tsArray,List<Double> dblArray) throws IOException{
    //用数组构造参数
    List<Entity> args = Arrays.asList(new BasicStringVector(strArray),new BasicIntVector(intArray),new BasicTimestampVector(tsArray),new BasicDoubleVector(dblArray));
    conn.run("tableInsert{sharedTable}", args);
}
```
在本例中，使用了DolphinDB 中的“部分应用”这一特性，将服务端表名以tableInsert{sharedTable}的方式固化到`tableInsert`中，作为一个独立函数来使用。具体文档请参考[部分应用文档](https://www.dolphindb.com/cn/help/PartialApplication.html)。

##### 7.1.3 使用`tableInsert`函数保存BasicTable对象

若Java程序获取的数据处理后组织成BasicTable对象，`tableInsert`函数也可以接受一个表对象作为参数，批量添加数据。

```java
public void test_save_table(BasicTable table1) throws IOException {
    List<Entity> args = Arrays.asList(table1);
    conn.run("tableInsert{shareTable}", args);
}
```

#### 7.2 保存数据到本地磁盘表

通常本地磁盘表用于学习环境或者单机静态数据集测试，它不支持事务，不持支并发读写，不保证运行中的数据一致性，所以不建议在生产环境中使用。

使用DolphinDB脚本创建一个数据表：
```
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

#### 7.3 保存数据到分布式表

分布式表是DolphinDB推荐在生产环境下使用的数据存储方式，它支持快照级别的事务隔离，保证数据一致性。分布式表支持多副本机制，既提供了数据容错能力，又能作为数据访问的负载均衡。

#### 7.3.1 使用`tableInsert`函数保存BasicTable对象

```
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

#### 7.3.2 分布式表的并发写入

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
//
```

首先，在DolphinDB服务端执行以下脚本，创建分布式数据库"dfs://DolphinDBUUID"和分布式表"device_status"。其中，数据库按照VALUE-HASH-HASH的组合进行三级分区。

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

> 请注意：DolphinDB不允许多个writer同时将数据写入到同一个分区，因此在客户端多线程并行写入数据时，需要确保每个线程分别写入不同的分区。

//todo: 使用最新的1.30版本以上的server，可以使用java api中的 PartitionedTableAppender类来写入分布式表，其基本原理是设计一个连接池用于多线程写入，然后利用server的schema函数获取分布式表的分区信息，按指定的分区列将用户写入的数据进行分类分别交给不同的连接来并行写入。
使用示例脚本如下：
```java 
DBConnectionPool conn = new ExclusiveDBConnectionPool(host, Integer.parseInt(port), "admin", "123456", Integer.parseInt(threadCount), false, false);

PartitionedTableAppender appender = new PartitionedTableAppender(dbPath, tableName, "gid", "saveGridData{'" + dbPath + "','" + tableName + "'}", conn);
BasicTable table1 = createTable();
appender.append(table1);            
```


对于按哈希值进行分区的分布式表， DolphinDB Java API提供了`HashBucket`函数来计算客户端数据的hash值。在客户端设计多线程并发写入分布式表时，根据哈希分区字段数据的哈希值分组，每组指定一个写线程。这样就能保证每个线程同时将数据写到不同的哈希分区。

```java
int key = areaUUID.hashBucket(10);
```

为了确保每个线程分别写入不同的分区，根据计划要分配的线程数，将预期的哈希值分成n组，保证各组中不包含相同哈希值。代码根据数据分区列的哈希值将之分到不同的组中。在生成数据时根据分区列的值将数据分流到不同组的队列中，这一步分流是并发写入的关键。

```java
Random rand = new Random();
BasicUuid areaUUID = areas.get(rand.nextInt(areas.size()));
BasicUuid deviceUUID = devices.get(rand.nextInt(devices.size()));
int key = areaUUID.hashBucket(10);
String groupId = getGroupId(key);

if (tbs.containsKey(groupId)) {
    if (tbs.get(groupId).currentTable == null) {
        tbs.get(groupId).currentTable = new BasicTableEx(createBasicTable(n));
    }
} else {
    throw new Exception("groupId is not exists");
}
```

当每组写入的数据容器中行数超出BATCHSIZE时，数据会打包成数据集加入到待写队列中，原数据容器清空等待重新写入。每组有各自的写数据线程，该线程会轮询各自的待写队列，将已入队的数据集提取出来并写入DolphinDB。

```java
if (tbs.get(groupId).currentTable.add(LocalDateTime.of(2020, 1, 1, 1, 1, 1, 1), areaUUID, deviceUUID, (double) (50 * rand.nextDouble()))) {
    if (tbs.get(groupId).currentTable.isFull()) {
        tbs.get(groupId).pushToQueue();
    }
}
```

开启DolphinDB消费线程。

```java
public class TaskConsumer implements Runnable {
    public void run() {
        for (String key : groups.keySet()) {
            new Thread(new DDBProxy(key)).start();
        }
    }
}
```

每个线程轮询各自的待写队列，将已入队的数据集提取出来并写入DolphinDB。

```java
public class DDBProxy implements Runnable {
    String _key = null;
    public DDBProxy(String key) {
        _key = key;
    }

    public void run() {
        do {
            if (tables.get(_key).TaskQueue.size() > 0) {
                BasicTable data = tables.get(_key).TaskQueue.poll();
                saveDataToDDB(data);
                insertRowCount += data.rows();
                if (insertRowCount < 2000 || insertRowCount > 90000)
                    System.out.println(String.format("insertRowCount = %s ; now is %s ", insertRowCount, LocalDateTime.now()));
            }
        } while (true);
    }
}
```

完整的并发写入案例可以参考[DFSWritingWithMultiThread.java](./example/DFSWritingWithMultiThread.java)。

#### 7.4 读取和使用数据表

#### 7.4.1 读取分布式表
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

#### 7.4.2 使用BasicTable对象
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

### 8. Java原生类型转换为DolphinDB数据类型

Java API提供了一组以Basic+\<DataType\>方式命名的类，分别对应DolphinDB的数据类型，比如BasicInt类，BasicDate类等等。

大部分DolphinDB数据类型可以由对应的Java数据类型构建，例如new BasicInt(4)对应integer，new BasicDouble(1.23)对应double，等等。但是也有一些DolphinDB数据类型，并不能由上述方法构建：

- CHAR类型：DolphinDB中的CHAR类型保存为一个byte，所以在Java API中用BasicByte类型来构造CHAR，例如new BasicByte((byte)'c')。
- SYMBOL类型：DolphinDB中的SYMBOL类型将字符串存储为整形，可以提高对字符串数据存储和查询的效率，但是Java中并没有这种类型，所以Java API不提供BasicSymbol这种对象，直接用BasicString来处理即可。
- 时间类型：DolphinDB的时间类型是以整形或者长整形来描述的，DolphinDB提供date, month, time, minute, second, datetime, timestamp, nanotime和nanotimestamp九种类型的时间类型，最高精度可以到纳秒级。具体的描述可以参考[DolphinDB时序类型和转换](https://www.dolphindb.com/cn/help/TemporalTypeandConversion.html)。由于Java也提供了LocalDate, LocalTime, LocalDateTime, YearMonth等数据类型，所以Java API在Utils类里提供了所有Java时间类型与int或long之间的转换函数。

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
也可以将DolphinDB对象转换为整形或长整形的时间戳，比如：
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

### 9. Java流数据API

Java程序可以通过API订阅流数据。Java API有两种获取数据的方式：

- 客户机上的应用程序定期去流数据表查询是否有新增数据，若有，应用程序会获取新增数据。

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

- Java API使用MessageHandler获取新数据。

首先需要调用者定义数据处理器handler。handler需要实现com.xxdb.streaming.client.MessageHandler接口。


```java
public class MyHandler implements MessageHandler {
       public void doEvent(IMessage msg) {
               BasicInt qty = msg.getValue(2);
               //..处理数据
       }
}
```

在启动订阅时，把handler实例作为参数传入订阅函数。

```java
ThreadedClient client = new ThreadedClient(subscribePort);
client.subscribe(serverIP, serverPort, tableName, new MyHandler(), offsetInt);
```

当流数据表有新增数据时，系统会通知Java API调用MyHandler方法，将新数据通过msg参数传入。

#### 断线重连

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

#### 启用filter

filter参数是一个向量。该参数需要发布端配合`setStreamTableFilterColumn`函数一起使用。使用`setStreamTableFilterColumn`指定流数据表的过滤列，流数据表过滤列在filter中的数据才会发布到订阅端，不在filter中的数据不会发布。

以下例子将一个包含元素1和2的整数类型向量作为`subscribe`的filter参数：

```java
BasicIntVector filter = new BasicIntVector(2);
filter.setInt(0, 1);
filter.setInt(1, 2);

PollingClient client = new PollingClient(subscribePort);
TopicPoller poller1 = client.subscribe(serverIP, serverPort, tableName, actionName, offset, filter);
```

#### 取消订阅
每一个订阅都有一个订阅主题topic作为唯一标识。如果订阅时topic已经存在，那么会订阅失败。这时需要通过unsubscribeTable函数取消订阅才能再次订阅。
```java
client.unsubscribe(serverIP, serverPort, tableName,actionName);
