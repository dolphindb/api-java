### 1. Java API 概念
Java API本质上实现了Java程序和DolphinDB服务器之间的消息传递和数据转换协议。
Java API运行在Java 1.8以上环境

### 2. Java对象和DolphinDB对象之间的映射
Java API遵循面向接口编程的原则。Java API使用接口类Entity来表示DolphinDB返回的所有数据类型。在Entity接口类的基础上，根据DolphinDB的数据类型，Java API提供了7种拓展接口，分别是scalar，vector，matrix，set，dictionary，table和chart。这些接口类都包含在com.xxdb.data包中。

拓展的接口类|命名规则|例子
---|---|---
scalar|Basic+<DataType>|BasicInt, BasicDouble, BasicDate, etc.
vector，matrix|Basic+<DataType>+<DataForm>|BasicIntVector, BasicDoubleMatrix, BasicAnyVector, etc.
set， dictionary和table|Basic+<DataForm>|BasicSet, BasicDictionary, BasicTable.
chart|BacisChart|

“Basic”表示基本的数据类型接口，<DataType>表示DolphinDB数据类型名称，<DataForm>是一个DolphinDB数据形式名称。

### 3. Java API提供的主要函数
DolphinDB Java API 提供的最核心的对象是DBConnection，它主要的功能就是让Java应用可以通过它调用DolphinDB的脚本和函数，在Java应用和DolphinDB服务器之间互通数据。
DBConnection类提供如下主要方法

| 方法名        | 详情          |
|:------------- |:-------------|
|connect(host, port, [username, password])|将会话连接到DolphinDB服务器|
|login(username,password,enableEncryption)|登陆服务器|
|run(script)|将脚本在DolphinDB服务器运行|
|run(functionName,args)|调用DolphinDB服务器上的函数|
|upload(variableObjectMap)|将本地数据对象上传到DOlphinDB服务器|
|isBusy()|判断当前会话是否正忙|
|close()|关闭当前会话|

### 4. 建立DolphinDB连接

Java API通过TCP/IP协议连接到Dolphin DB服务器。 在下列例子中，我们连接正在运行的端口号为8848的本地DolphinDB服务器：

```
import com.xxdb;
DBConnection conn = new DBConnection();
boolean success = conn.connect("localhost", 8848);
```
使用用户名和密码建立连接：

```
boolean success = conn.connect("localhost", 8848, "admin", "123456");
```

### 5.运行脚本

在Java中运行DolphinDB脚本的语法如下：
```
conn.run("script");
```
其中，脚本的最大长度为65,535字节。

如果脚本只包含一条语句，如表达式，DolphinDB会返回一个数据对象；否则返回NULL对象。如果脚本包含多条语句，将返回最后一个对象。如果脚本含有错误或者出现网络问题，它会抛出IOException。

### 6.操作DolphinDB数据结构的数据

下面介绍建立DolphinDB连接后，在Java环境中，对不同DolphinDB数据类型进行操作，运行结果显示在Console窗口。

首先导入DolphinDB数据类型包：

```
import com.xxdb.data.*;
```

注意，下面的代码紧接建立DolphinDB连接的例子。

- 向量

在下面的示例中，DolphinDB语句
```
rand(`IBM`MSFT`GOOG`BIDU,10)
```
返回Java对象BasicStringVector。vector.rows()方法能够获取向量的大小。我们可以使用vector.getString(i)方法按照索引访问向量元素。

```
public void testStringVector() throws IOException{

    BasicStringVector vector = (BasicStringVector)conn.run("rand(`IBM`MSFT`GOOG`BIDU, 10)");

    int size = vector.rows();
    System.out.println("size: "+size);

    for(int i=0; i<size; ++i)
        System.out.println(vector.getString(i));
}
```

类似的，也可以处理双精度浮点类型的向量或者元组。
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

要从整数矩阵中检索一个元素，我们可以使用getInt(row,)。 要获取行数和列数，我们可以使用函数rows()和columns()。

```
public void testIntMatrix() throws IOException {

       BasicIntMatrix matrix = (BasicIntMatrix)conn.run("1..6$3:2");

       System.out.println(matrix.getString());

}
```

- 字典

用函数keys()和values()可以从字典取得所有的键和值。要从一个键里取得它的值，可以调用get(key)。

```
public void testDictionary() throws IOException{

    BasicDictionary dict = (BasicDictionary)conn.run("dict(1 2 3,`IBM`MSFT`GOOG)");

    //to print the corresponding value for key 1.        

    System.out.println(dict.get(new BasicInt(1)).getString());

}
```


- 表

要获取表的列，我们可以调用table.getColumn(index)；同样，我们可以调用table.getColumnName(index)获取列名。 对于列和行的数量，我们可以分别调用table.columns()和table.rows()。

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

要描述一个NULL对象，我们可以调用函数obj.getDataType()。
```
public void testVoid() throws IOException{

       Entity obj = conn.run("NULL");

       System.out.println(obj.getDataType());

}
```


### 7.调用DolphinDB函数

调用的函数可以是内置函数或用户自定义函数。 下面的示例将一个double向量传递给服务器，并调用sum函数。

```
import java.util.List;
import java.util.ArrayList;
//Run DolphinDB function with Java objects
public void testFunction() throws IOException{

    List<Entity> args = new ArrayList<Entity>(1);
    BasicDoubleVector vec = new BasicDoubleVector(3);
    vec.setDouble(0, 1.5);
    vec.setDouble(1, 2.5);
    vec.setDouble(2, 7);
    args.add(vec);

    Scalar result = (Scalar)conn.run("sum", args);
    System.out.println(result.getString());

}
```

### 8.将对象上传到DolphinDB服务器

我们可以将二进制数据对象上传到DolphinDB服务器，并将其分配给一个变量以备将来使用。 变量名称可以使用三种类型的字符：字母，数字或下划线。 第一个字符必须是字母。

```
//Run DolphinDB function with Java objects

public void testFunction() throws IOException{

    List<Entity> args = new ArrayList<Entity>(1);
    List<Entity> vars = new ArrayList<String>(1);
    BasicDoubleVector vec = new BasicDoubleVector(3);
    vec.setDouble(0, 1.5);
    vec.setDouble(1, 2.5);
    vec.setDouble(2, 7);
    args.add(vec);
    vars.add("a");

    conn.run(vars,args);
    Entity result = conn.run("accumulate(+,a)");
    System.out.println(result.getString());

}
```

### 9. 如何将java数据表对象保存到DolphinDB的数据库中
DolphinDB的数据表按存储方式分为内存表，数据仅保存在本节点内存，存取速度最快，但是节点关闭数据就不存在了。
本地磁盘表：数据保存在本地磁盘上，即使节点关闭，通过脚本就可以方便的从磁盘加载到内存。
分布式表：数据在物理上分布在不同的节点，通过DolphinDB的分布式计算引擎，逻辑上仍然可以像本地表一样做统一查询。
下面展示通过java api将表上传到DolphinDB的不同类型的数据表中。
示例中使用`createBasicTable()`函数模拟一个的BasicTable表，此函数的主题内容在附录中提供
#### 9.1. 将BasicTable保存到内存表
java程序将需要的业务信息组织成BasicTable对象后，通过`DBConnection.run (functionName,args)`方法，将本地对象保存到到DolphinDB服务器的内存表中。由于内存表示会话间不可见的，所以为了能够在GUI中查看上传的数据，使用了`share`关键字将内存表在会话间共享。

```
public void test_save_memoryTable() throws IOException{
	BasicTable table1 = createBasicTable();
	conn.run("t = table(10000:0,`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring,[BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING])\n");
	conn.run("share t as memoryTable");
	conn.run("def saveData(data){ memoryTable.append!(data)}");
	List<Entity> args = new ArrayList<Entity>(1);
	args.add(table1);
	conn.run("saveData", args);
}
```
#### 9.2. 将BasicTable保存到本地磁盘表
本示例将数据保存在DolphinDB服务器的磁盘上，按照symbol字段对数据进行了简单的分区。
```
public void test_save_localTable() throws IOException{
	BasicTable table1 = createBasicTable();
	String dbpath = "/home/data/testDatabase";
	conn.run("t = table(10000:0,`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring,[BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING])\n");
	conn.run(String.format("if(existsDatabase('{0}')){dropDatabase('{0}')}",dbpath));
	conn.run(String.format("db = database('{0}',VALUE,'MS' 'GOOG' 'FB')",dbpath));
	conn.run("db.createPartitionedTable(t,'tb1','csymbol')");
	conn.run(String.format("def saveData(data){ loadTable('{0}','tb1').append!(data)}",dbpath));
	List<Entity> args = new ArrayList<Entity>(1);
	args.add(table1);
	conn.run("saveData", args);
}
```

#### 9.3. 将BasicTable保存到分布式表
本示例将数据保存到DolphinDB服务器的分布式数据库中，例子中按照日期字段对数据进行一级分区，要了解更多的分区方式可以参考[数据库分区教程](https://github.com/dolphindb/Tutorials_CN/blob/master/database.md)。

```
public void test_save_dfsTable() throws IOException{
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
}
```

### 10. java api对象与java对象之间的格式转换
本示例循环取出BasicTable中的每一个值，并转换成java的原生对象输出。

```
public void test_loop_basicTable() throws Exception{
	BasicTable table1 = createBasicTable();
	for(int ri=0;ri<table1.rows();ri++){
		BasicBoolean boolv = (BasicBoolean)table1.getColumn("cbool").get(ri);
		System.out.println(boolv.getBoolean());

		BasicByte charv = (BasicByte)table1.getColumn("cchar").get(ri);
		System.out.println((char)charv.getByte());

		BasicShort shortv = (BasicShort) table1.getColumn("cshort").get(ri);
		System.out.println(shortv.getNumber().shortValue());

		BasicInt intv = (BasicInt) table1.getColumn("cint").get(ri);
		System.out.println(intv.getNumber().intValue());

		BasicLong longv = (BasicLong) table1.getColumn("clong").get(ri);
		System.out.println(longv.getNumber().longValue());

		BasicDate datev = (BasicDate) table1.getColumn("cdate").get(ri);
		LocalDate date = datev.getDate();
		System.out.println(date);

		BasicMonth monthv = (BasicMonth) table1.getColumn("cmonth").get(ri);
		YearMonth ym = monthv.getMonth();
		System.out.println(ym);

		BasicTime timev = (BasicTime) table1.getColumn("ctime").get(ri);
		LocalTime time = timev.getTime();
		System.out.println(time);

		BasicMinute minutev = (BasicMinute) table1.getColumn("cminute").get(ri);
		LocalTime minute = minutev.getMinute();
		System.out.println(minute);

		BasicSecond secondv = (BasicSecond) table1.getColumn("csecond").get(ri);
		LocalTime second = secondv.getSecond();
		System.out.println(second);

		BasicDateTime datetimev = (BasicDateTime) table1.getColumn("cdatetime").get(ri);
		LocalDateTime datetime = datetimev.getDateTime();
		System.out.println(datetime);

		BasicTimestamp timestampv = (BasicTimestamp) table1.getColumn("ctimestamp").get(ri);
		LocalDateTime timestamp = timestampv.getTimestamp();
		System.out.println(timestamp);

		BasicNanoTime nanotimev = (BasicNanoTime) table1.getColumn("cnanotime").get(ri);
		LocalTime nanotime = nanotimev.getNanoTime();
		System.out.println(nanotime);

		BasicNanoTimestamp nanotimestampv = (BasicNanoTimestamp) table1.getColumn("cnanotimestamp").get(ri);
		LocalDateTime nanotimestamp = nanotimestampv.getNanoTimestamp();
		System.out.println(nanotimestamp);

		BasicFloat floatv = (BasicFloat) table1.getColumn("cfloat").get(ri);
		System.out.println(floatv.getFloat());

		BasicDouble doublev = (BasicDouble) table1.getColumn("cdouble").get(ri);
		System.out.println(doublev.getDouble());

		BasicString symbolv = (BasicString) table1.getColumn("csymbol").get(ri);
		System.out.println(symbolv.getString());

		BasicString stringv = (BasicString) table1.getColumn("cstring").get(ri);
		System.out.println(stringv.getString());
	}
}
```