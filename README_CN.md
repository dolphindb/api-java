### 1. Java API 概述
Java API需要运行在Java 1.8或以上环境。

Java API遵循面向接口编程的原则。Java API使用接口类Entity来表示DolphinDB返回的所有数据类型。在Entity接口类的基础上，根据DolphinDB的数据类型，Java API提供了7种拓展接口，分别是scalar，vector，matrix，set，dictionary，table和chart。这些接口类都包含在com.xxdb.data包中。

拓展的接口类|命名规则|例子
---|---|---
scalar|`Basic<DataType>`|BasicInt, BasicDouble, BasicDate, etc.
vector, matrix|`Basic<DataType><DataForm>`|BasicIntVector, BasicDoubleMatrix, BasicAnyVector, etc.
set, dictionary, table|`Basic<DataForm>`|BasicSet, BasicDictionary, BasicTable.
chart||BasicChart

"Basic"表示基本的数据类型接口，`<DataType>`表示DolphinDB数据类型名称，`<DataForm>`是一个DolphinDB数据形式名称。接口和类的详细描述请参考[Java API手册](https://www.dolphindb.com/javaapi/)。

DolphinDB Java API提供的最核心的对象是DBConnection。Java应用可以通过它在DolphinDB服务器上执行脚本和函数，并在两者之间双向传递数据。DBConnection类提供如下主要方法：

| 方法名        | 详情          |
|:------------- |:-------------|
|connect(host, port, [username, password])|将会话连接到DolphinDB服务器|
|login(username,password,enableEncryption)|登陆服务器|
|run(script)|将脚本在DolphinDB服务器运行|
|run(functionName,args)|调用DolphinDB服务器上的函数|
|upload(variableObjectMap)|将本地数据对象上传到DolphinDB服务器|
|isBusy()|判断当前会话是否正忙|
|close()|关闭当前会话|

### 2. 建立DolphinDB连接

Java API通过TCP/IP协议连接到DolphinDB服务器。在以下例子中，我们连接正在运行的端口号为8848的本地DolphinDB服务器：

```
import com.xxdb;
DBConnection conn = new DBConnection();
boolean success = conn.connect("localhost", 8848);
```

使用用户名和密码建立连接：
```
boolean success = conn.connect("localhost", 8848, "admin", "123456");
```

若未使用用户名及密码连接成功，则脚本在Guest权限下运行。后续运行中若需要提升权限，可以通过conn.login('admin','123456',true)登录获取权限。

### 3.运行脚本

在Java中运行DolphinDB脚本：
```
conn.run("script");
```
脚本的最大长度为65,535字节。

### 4. 运行函数

可使用`run`命令在远程DolphinDB服务器上执行DolphinDB内置或用户自定义函数。

下面的示例展示Java程序调用DolphinDB内置的`add`函数。`add`函数有两个参数x和y。参数的存储位置不同，也会导致调用方式的不同。可能有以下三种情况：

* 所有参数都在DolphinDB server端

若变量x和y已经通过Java程序在服务器端生成，
```
conn.run("x = [1,3,5];y = [2,4,6]")
```
那么在Java端要对这两个向量做加法运算，只需直接使用run("script")即可。
```
public void testFunction() throws IOException{
    Vector result = (Vector)conn.run("add(x,y)");
    System.out.println(result.getString());
}
```

* 仅有一个参数在DolphinDB server端存在

若变量x已经通过Java程序在服务器端生成，
```
conn.run("x = [1,3,5]")
```
而参数y要在Java客户端生成，这时就需要使用“部分应用”方式，把参数x固化在`add`函数内。具体请参考[部分应用文档](https://www.dolphindb.com/cn/help/PartialApplication.html)。

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
* 两个参数都待由Java客户端赋值
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

### 5. 上传数据对象

可使用`upload`方法，将本地的数据上传到DolphinDB服务器并分配给一个变量。变量名称可以使用三种类型的字符：字母，数字或下划线，第一个字符必须是字母。

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

### 6. 读取数据示例

下面介绍通过DBConnection对象，读取DolphinDB不同类型的数据。

首先导入DolphinDB数据类型包：

```
import com.xxdb.data.*;
```

- 向量

以下DolphinDB语句返回Java对象BasicStringVector。

```
rand(`IBM`MSFT`GOOG`BIDU,10)
```
`rows`方法可以获取向量的元素数量。我们可以使用`getString`方法按照索引访问向量元素。

```
public void testStringVector() throws IOException{
    BasicStringVector vector = (BasicStringVector)conn.run("rand(`IBM`MSFT`GOOG`BIDU, 10)");
    int size = vector.rows();
    System.out.println("size: "+size);
    for(int i=0; i<size; ++i)
        System.out.println(vector.getString(i));
}
```

类似的，也可以处理其它数据类型的向量或者元组。
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

- 集合

```
public void testSet() throws IOException{
    BasicSet result = (BasicSet)conn.run("set(1+3*1..100)");
    System.out.println(result.getString());
}
```

- 矩阵

要从整数矩阵中检索一个元素，可以使用`getInt`。 要获取行数和列数，可以使用函数`rows`和`columns`。

```
public void testIntMatrix() throws IOException {
    BasicIntMatrix matrix = (BasicIntMatrix)conn.run("1..6$3:2");
    System.out.println(matrix.getString());
}
```

- 字典

用函数`keys`和`values`可以从字典取得所有的键和值。要从一个键里取得它的值，可以调用`get`。

```
public void testDictionary() throws IOException{
    BasicDictionary dict = (BasicDictionary)conn.run("dict(1 2 3,`IBM`MSFT`GOOG)");
    //to print the corresponding value for key 1.
    System.out.println(dict.get(new BasicInt(1)).getString());
}
```

- 表

要获取一个表中某列，可以用`table.getColumn(index)`。使用`table.columns()`和`table.rows()`来分别获取一个表的列数和行数。

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
- NULL对象

要判断一个对象是否为NULL，我们可以使用`obj.getDataType()`。
```
public void testVoid() throws IOException{
    Entity obj = conn.run("NULL");
    System.out.println(obj.getDataType());
}
```

### 7. 读写DolphinDB数据表

使用Java API的一个重要场景是，用户从其他数据库系统或是第三方Web API中取得数据后存入DolphinDB数据库中。本节将介绍通过Java API将取到的数据上传并保存到DolphinDB的数据表中。

DolphinDB数据表按存储方式分为三种:

- 内存表: 数据仅保存在内存中，存取速度最快，但是节点关闭后数据就不存在了。
- 本地磁盘表：数据保存在本地磁盘上。可以从磁盘加载到内存。
- 分布式表：数据分布在不同的节点，通过DolphinDB的分布式计算引擎，仍然可以像本地表一样做统一查询。

#### 7.1 保存数据到DolphinDB内存表

DolphinDB提供多种方式来保存数据：
- 通过`insert into`保存单条数据
- 通过`tableInsert`函数批量保存多条数据
- 通过`tableInsert`函数保存数据表

一般不建议通过`append!`函数保存数据，因为`append!`函数会返回一个表结构，增加通信量。

下面分别介绍三种方式保存数据的实例，在例子中使用到的数据表有4个列，分别是string, int, timestamp, double类型，列名分别为cstring, cint, ctimestamp, cdouble。
```
t = table(10000:0,`cstring`cint`ctimestamp`cdouble,[STRING,INT,TIMESTAMP,DOUBLE])
share t as sharedTable
```
由于内存表是会话隔离的，所以该内存表只有当前会话可见。如果需要在其它会话中访问，需要通过`share`在会话间共享内存表。

##### 7.1.1 使用`INSERT INTO`保存单条数据

若每次将单条数据记录保存到DolphinDB，可以使用SQL语句`INSERT INTO`。
```
public void test_save_Insert(String str,int i, long ts,double dbl) throws IOException{
    conn.run(String.format("insert into sharedTable values('%s',%s,%s,%s)",str,i,ts,dbl));
}
```

##### 7.1.2 使用`tableInsert`函数批量保存数组对象

若Java程序获取的数据可以组织成List方式，`tableInsert`函数比较适合用来批量保存多条数据。这个函数可以接受多个数组作为参数，将数组追加到数据表中。

```
public void test_save_TableInsert(List<String> strArray,List<Integer> intArray,List<Long> tsArray,List<Double> dblArray) throws IOException{
    //用数组构造参数
    List<Entity> args = Arrays.asList(new BasicStringVector(strArray),new BasicIntVector(intArray),new BasicTimestampVector(tsArray),new BasicDoubleVector(dblArray));
    conn.run("tableInsert{sharedTable}", args);
}
```
在本例中，使用了DolphinDB 中的“部分应用”这一特性，将服务端表名以tableInsert{sharedTable}的方式固化到`tableInsert`中，作为一个独立函数来使用。具体文档请参考[部分应用文档](https://www.dolphindb.com/cn/help/PartialApplication.html)。

##### 7.1.3 使用`tableInsert`函数保存BasicTable对象

若Java程序获取的数据处理后组织成BasicTable对象，tableInsert函数也可以接受一个表对象作为参数，批量添加数据。

```
public void test_save_table(BasicTable table1) throws IOException {
    List<Entity> args = Arrays.asList(table1);
    conn.run("tableInsert{shareTable}", args);
}
```
#### 7.2 保存数据到分布式表

分布式表是DolphinDB推荐在生产环境下使用的数据存储方式，它支持快照级别的事务隔离，保证数据一致性。分布式表支持多副本机制，既提供了数据容错能力，又能作为数据访问的负载均衡。

请注意只有启用enableDFS=1的集群环境才能使用分布式表。

```
dbPath = 'dfs://testDatabase'
tbName = 'tb1'

if(existsDatabase(dbPath)){dropDatabase(dbPath)}
db = database(dbPath,RANGE,2018.01.01..2018.12.31)
db.createPartitionedTable(t,tbName,'ctimestamp')
```
DolphinDB提供`loadTable`方法可以加载分布式表，通过`tableInsert`方式追加数据，具体的脚本示例如下：

```
public void test_save_table(String dbPath, BasicTable table1) throws IOException{
    List<Entity> args = new ArrayList<Entity>(1);
    args.add(table1);
    conn.run(String.format("tableInsert{loadTable('%s','tb1')}",dbPath), args);
}
```

当用户在Java程序中取到的值是数组或列表时，也可以很方便的构造出BasicTable用于追加数据，例如若有 boolArray, intArray, dblArray, dateArray, strArray 5个列表对象(List<T>),可以通过以下语句构造BasicTable对象：

```
List<String> colNames =  Arrays.asList("cbool","cint","cdouble","cdate","cstring");
List<Vector> cols = Arrays.asList(new BasicBooleanVector(boolArray),new BasicIntVector(intArray),new BasicDoubleVector(dblArray),new BasicDateVector(dateArray),new BasicStringVector(strArray));
BasicTable table1 = new BasicTable(colNames,cols);
```

#### 7.3 保存数据到本地磁盘表

本地磁盘表通用用于静态数据集的计算分析，既可以用于数据的输入，也可以作为计算的输出。它不支持事务，也不持支并发读写。

使用DolphinDB脚本创建一个数据表：
```
dbPath = "C:/data/testDatabase"
tbName = 'tb1'

if(existsDatabase(dbPath)){dropDatabase(dbPath)}
db = database(dbPath,RANGE,2018.01.01..2018.12.31)
db.createPartitionedTable(t,tbName,'ctimestamp')
```
DolphinDB提供`loadTable`方法同样可以加载本地磁盘表，通过`tableInsert`追加数据。
```
public void test_save_table(String dbPath, BasicTable table1) throws IOException{
    List<Entity> args = new ArrayList<Entity>(1);
    args.add(table1);
    conn.run(String.format("tableInsert{loadTable('%s','tb1')}",dbPath), args);
}
```
#### 7.4 读取和使用数据表

在Java API中，数据表保存为BasicTable对象。由于BasicTable是列式存储，所以若要在Java API中读取行数据需要先取出需要的列，再取出行。

例子中参数BasicTable的有4个列，列名分别为cstring, cint, ctimestamp, cdouble，数据类型分别是STRING, INT, TIMESTAMP, DOUBLE。

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

### 8. Java原生类型向DolphinDB数据类型转换

Java API提供了一组以Basic+\<DataType\>方式命名的类，分别对应DolphinDB的数据类型，比如BasicInt类，BasicDate类等等。

大部分Java原生类型可以通过构造函数直接创建对应的DolphinDB数据类型，比如new BasicInt(4)，new BasicDouble(1.23)。但是也有一些特殊的DolphinDB数据类型，并不能按前面所描述的方法简单转换，下面针对这些类型做出说明：

大部分DolphinDB数据类型可以由Java原生类型构建，例如new BasicInt(4)对应integer，new BasicDouble(1.23)对应double，等等。但是也有一些DolphinDB数据类型，并不能由上述方法构建：

- CHAR类型：DolphinDB中的CHAR类型以Byte形式保存，所以在Java API中用BasicByte类型来构造CHAR，例如new BasicByte((byte)'c')。
- SYMBOL类型：DolphinDB中的SYMBOL类型是对字符串的优化，可以提高DolphinDB对字符串数据存储和查询的效率，但是Java中并不需要这种类型，所以Java API不提供BasicSymbol这种对象，直接用BasicString来处理即可。
- 时间类型：DolphinDB的时间类型是以整形或者长整形来描述的，DolphinDB提供date、month、time、minute、second、datetime、timestamp、nanotime、nanotimestamp九种类型的时间类型，最高精度可以到纳秒级。具体的描述可以参考[DolphinDB时序类型和转换](https://www.dolphindb.com/cn/help/TemporalTypeandConversion.html)。由于Java也提供了LocalDate、LocalTime、LocalDateTime、YearMonth等数据类型，所以Java API在Utils类里提供了所有Java时间类型与int或long之间的转换函数。

以下脚本展示Java API中DolphinDB时间类型与Java原生时间类型之间的对应关系：
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
BasicNanoTimestamp bnts = new BasicNanoTimestamp(LocalDateTime.of(2018,11,12,8,1,1,123456789));
```
如果在第三方系统中时间以时间戳的方式存储，DolphinDB时间对象也可以用时间戳来实例化。Java API中的Utils类提供了各种时间类型与标准时间戳的转换算法，比如将毫秒级的时间戳转换为DolphinDB的BasicTimestamp对象:
```
LocalDateTime dt = Utils.parseTimestamp(1543494854000l);
BasicTimestamp ts = new BasicTimestamp(dt);
```
也可以将DolphinDB对象转换为整形或长整形的时间戳，比如：
```
LocalDateTime dt = ts.getTimestamp();
long timestamp = Utils.countMilliseconds(dt);
```
如果时间戳以其他精度保存，Utils类还中提供如下方法，可以适应各种不同的精度：
- Utils.countMonths：计算给定时间到1970.01之间的月份差，返回int
- Utils.countDays：计算给定时间到1970.01.01之间的天数差，返回int
- Utils.countMinutes：计算给定时间到1970.01.01T00:00之间的分钟差，返回int
- Utils.countSeconds：计算给定时间到1970.01.01T00:00:00之间的秒数差，返回int
- Utils.countMilliseconds：计算给定时间到1970.01.01T00:00:00之间的毫秒数差，返回long
- Utils.countNanoseconds：计算给定时间到1970.01.01T00:00:00.000之间的纳秒数差，返回long

### 9. Java流数据API

Java程序可以通过API订阅流数据，当数据进入客户端后，Java API有两种处理数据的方式：

- 客户机上的应用程序定期检查是否添加了新数据。如果添加了新数据，应用程序会获取数据并且在工作中使用它们。

```
PollingClient client = new PollingClient(subscribePort);
TopicPoller poller1 = client.subscribe(serverIP, serverPort, tableName, offset);

while (true) {
   ArrayList<IMessage> msgs = poller1.poll(1000);
   if (msgs.size() > 0) {
         BasicInt value = msgs.get(0).getEntity(2);  //取数据中第一行第二个字段
   }
}
```

每次流数据表发布新数据时，poller1会拉取到新数据。无新数据发布时，程序会阻塞在poller1.poll方法这里等待。

Java API使用预先设定的MessageHandler获取及处理新数据。首先需要调用者定义数据处理器Handler，Handler需要实现com.xxdb.streaming.client.MessageHandler接口。

- Java API使用预先设定的MessageHandler直接使用新数据。

```
public class MyHandler implements MessageHandler {
       public void doEvent(IMessage msg) {
               BasicInt qty = msg.getValue(2);
               //..处理数据
       }
}
```

在启动订阅时，把handler实例作为参数传入订阅函数。

```
ThreadedClient client = new ThreadedClient(subscribePort);
client.subscribe(serverIP, serverPort, tableName, new MyHandler(), offsetInt);
```

当每次流数据表有新数据发布时，Java API会调用MyHandler方法，并将新数据通过msg参数传入。

#### 断线重连

`reconnect`参数是一个布尔值，表示订阅意外中断后，是否会自动重新订阅。默认值为`false`。如果`reconnect=true`，有以下三种情况：

- 如果发布端与订阅端处于正常状态，但是网络中断，那么订阅端会在网络正常时，自动从中断位置重新订阅。
- 如果发布端崩溃，订阅端会在发布端重启后不断尝试重新订阅。
    - 如果发布端对流数据表启动了持久化，发布端重启后会首先读取硬盘上的数据，直到发布端读取到订阅中断位置的数据，订阅端才能成功重新订阅。
    - 如果发布端没有对流数据表启用持久化，那么订阅端将自动重新订阅失败。
- 如果订阅端崩溃，订阅端重启后不会自动重新订阅，需要重新执行`subscribe`函数。

以下例子在订阅时，设置`reconnect`为`true`：

```
PollingClient client = new PollingClient(subscribePort);
TopicPoller poller1 = client.subscribe(serverIP, serverPort, tableName, offset, true);
```

#### 启用filter

`filter`参数是一个向量。该参数需要发布端配合`setStreamTableFilterColumn`函数一起使用。使用`setStreamTableFilterColumn`指定流数据表的过滤列，流数据表过滤列在`filter`中的数据才会发布到订阅端，不在`filter`中的数据不会发布。

以下例子将一个包含元素1和2的整数类型向量作为`subscribe`的`filter`参数：

```
BasicIntVector filter = new BasicIntVector(2);
filter.setInt(0, 1);
filter.setInt(1, 2);

PollingClient client = new PollingClient(subscribePort);
TopicPoller poller1 = client.subscribe(serverIP, serverPort, tableName, actionName, offset, filter);
```
