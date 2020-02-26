## DolphinDB java API例子

### 1. 概述
目前已有3个java API的例子，如下表所示：

| 例子主题        | 文件名称          |
|:-------------- |:-------------|
|数据库写入|DFSTableWriting.java|
|多线程并行写入数据库|DFSWritingWithMultiThread.java|
|流数据写入和订阅|streamingData.java|

本文下面对每个例子分别进行简单的说明，包括运行和使用帮助等。
### 2. 数据库写入例子

本例实现了用Java API往分布式数据库写入数据的功能。例子中的目标数据库是一个按日期分区的分布式数据库。

#### 2.1 代码说明
主要有2个函数：
* createBasicTable函数 : 定义写入的数据，该函数创建了一个本地的表对象BasicTable。
* writeDfsTable函数 : 通过API在DolphinDB创建待写入的分布式表，并用`run("tableInsert",args)`函数将Java端的BasicTable上传和写入分布式表。
#### 2.2 运行
将代码打包成xxx.jar 执行 
```
java -jar xxx.jar [serverIP] [serverPort]
```
若不传入serverIP和serverPort参数，默认serverIP="localhost"，serverPort==8848
### 3. 数据库多线程并行写入例子
当数据写入流量较大时，需要使用并行写入的方式来提升IO。DolphinDB中不允许多个线程并行写入同一个分区，所以要从整体上实现并行写入，需要确保每个线程分别写入不同分区的数据。

本例以一个VALUE-HASH-HASH三层分区数据库作为写入目标，实现了如何在Java应用中将数据并行写入DolphinDB分布式表。

* 数据库分区脚本
```
login("admin","123456")
dbName="dfs://DolphinDBUUID"
tableName="device_status"
db1=database("",VALUE, 2019.11.01..2020.02.01)
db2=database("",HASH,[UUID,10])
db3=database("",HASH,[UUID,10])
t = table(200:0,`time`areaId`deviceId`value,[TIMESTAMP,UUID,UUID,DOUBLE])
db = database(dbName, COMPO,[db1,db2,db3])
db.createPartitionedTable(t, tableName,  `time`areaId`deviceId)
```

#### 3.1 代码说明

根据计划要分配的线程数，将预期的哈希值分成n组，保证各组中不包含相同哈希值。代码根据数据分区列的哈希值将之分到不同的组中。**在生成数据时根据分区列的值将数据分流到不同组的队列中，这一步分流是并发写入的关键，同样原理可以用于其他分区方式**。
在实现上，每组对应一个并发线程。当哈希分区数比较多的时候，可以通过这种分组的方式控制线程数。

当每组写入的数据容器中行数超出BATCHSIZE时，数据会打包成数据集加入到待写队列中，原数据容器清空等待重新写入。每组有各自的写数据线程，该线程会轮询各自的待写队列，将已入队的数据集提取出来并写入DolphinDB。

主要有几个函数和类如下：

* createBasicTable函数：创建本地BasicTable对象作为数据容器。

* generateData函数：生成客户端数据并加入消费队列, 内部循环调用generateOneRow。

* generateOneRow函数：用于产生每一条模拟数据并分流。

* DBTaskItem类：将写入队列中满足BatchSize的数据集加入待写入队列中。

* TaskConsumer类：消费线程类，根据哈希值的分组情况，为每个组开启写线程。

* DDBProxy类：写入线程类，负责将每一组的待写队列写入DolphinDB。

#### 3.2 运行
将代码打包成xxx.jar执行

```
java -jar xxx.jar [batchSize] [freq] [serverIP] [serverPort]
```
batchSize表示每次加入数据队列的数据量，freq表示并发写入次数，总写入数据量为batchSize*freq

若不传入参数，默认batchSize=2000，freq=50，serverIP="localhost"，serverPort==8848

### 4. 流数据写入和订阅例子
Java API提供了ThreadedClient、ThreadPooledClient和PollingClient三种订阅模式订阅流表的数据。三种模式的主要区别在于收取数据的方式：

ThreadedClient单线程执行，并且对收到的消息直接执行用户定义的handler函数进行处理；

ThreadPooledClient多线程执行，对收到的消息进行多线程并行调用用户定义的handler函数进行处理；

PollingClient返回一个消息队列，用户可以通过轮询的方式获取和处理数据。

在本例流数据订阅的源代码中，选择用ThreadedClient，PollingClient两种方式。ThreadPooledClient可以参照ThreadedClient使用方式。

#### 4.1 代码说明
本例实现了流数据表的写入和流数据订阅的功能，订阅服务端发布的数据并在Java应用端打印出来，主要有5个函数和类如下：
* writeStreamTable函数，用于创建流数据表，以及用run函数运行tableInsert脚本把模拟数据写入流表，并显示写入的结果。
* createBasicTable函数，定义待写入的数据，该函数创建了一个本地的表对象BasicTable。
* pollingClient类，用PollingClient订阅模式订阅流表的数据，并在主线程中获取的数据展示出来。
* ThreadedClient类，用ThreadedClient订阅模式订阅流表的数据。
* SampleMessageHandler类，处理ThreadedClient订阅的流表数据，将获取的数据展示出来

#### 4.2 运行
将代码打包成xxx.jar 执行 
```
java -jar xxx.jar [serverIP] [serverPort] [subscribePort] [subscribeMethod]
```
subscribeMethod有2个选项：
* 'T'，用ThreadedClient开启订阅。
* 'P'，用PollingClient开启订阅。

若不传入参数，默认serverIP="localhost"，serverPort==8848，subscribePort=8892，subscribeMethod='P'.

程序运行之后，Server端流表被创建，客户端处于等待流数据的状态。在DolphinDB 服务端运行以下脚本发布数据(持续100秒)：
```
for(x in 0:1000){
    time =time(now())
    sym= rand(`S`M`MS`GO, 1)
    qty= rand(1000..2000,1)
    price = rand(2335.34,1)
    insert into Trades values(x, time,sym, qty, price)
    sleep(100)
}
```