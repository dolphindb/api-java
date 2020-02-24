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
#### 2.1 代码说明
本例实现了用单线程往分布式数据库写入数据的功能。本例子中使用了append往表中批量插入数据。在源代码中，主要有2个函数：
* createBasicTable函数，定义写入的数据，该函数创建了一个本地的表对象BasicTable。
* writeDfsTable函数，通过API在DolphinDB创建待写入的分布式表，并用`run("tableInsert",args)`函数将Java端的BasicTable上传和写入分布式表。
#### 2.2 运行
将代码打包成xxx.jar 执行 
```
java -jar xxx.jar [serverIP] [serverPort]
```
若不传入serverIP和serverPort参数，默认serverIP="localhost"，serverPort==8848
### 3. 数据库多线程并行写入例子
在数据流量非常大的场景，需要使用并行写入的方式来提升IO，DolphinDB中同一个分区不允许多个线程同时并行写入，要实现并行写入，需要确保每个线程分别写入不同的分区。本例以如下数据库分区为例，实现了如何在Java应用中将数据并行写入DolphinDB分布式表。

数据库结构为日期-hash-hash三层分区，数据流量为一天250G左右。如果数据流量更大，可以将第一层分区调整为小时(DateHour),并且可以调整每一层hash分区的数量。
```
login("admin","123456")
dbName="dfs://DolphinDBUUID"
tableName="device_status"
db1=database("",VALUE, 2019.11.01..2020.02.01)
db2=database("",HASH,[UUID,50])
db3=database("",HASH,[UUID,50])
t = table(200:0,`time`areaId`deviceId`value,[TIMESTAMP,UUID,UUID,DOUBLE])
db = database(dbName, COMPO,[db1,db2,db3])
db.createPartitionedTable(t, tableName,  `time`areaId`deviceId)
```

#### 3.1 代码说明
主线程采集数据，并根据分区列数据的哈希值将数据分流到对应组的队列中，每组的写线程会轮询各自的数据队列，将入队的数据提取出来并写入DolphinDB。主要有几个函数和类如下：
* generateData函数，用于定义每组的数据队列，开启写DolphinDB消费线程，循环生成客户端数据并加入消费队列。
* generateOneRow函数，用于产生每一条模拟数据，并根据数据首个hash分区列的哈希值将数据分流到对应组的队列中。
* createBasicTable函数，创建本地BasicTable对象作为数据容器。
* BasicTableEx类，对于BasicTable的扩展，维护一个当前数据量，当数据量达到指定大小时，将数据提交写入线程。
* DBTaskItem类，将数据满了的本地表对象加入数据队列中。
* TaskConsumer类，Java消费线程，根据哈希值的分组情况，为每个组开启写线程。
* DDBProxy类，负责每一组的数据写入DolphinDB。

#### 3.2 运行
将代码打包成xxx.jar执行

```
java -jar xxx.jar [batchSize] [freq] [serverIP] [serverPort]
```
batchSize表示每次加入数据队列的数据量，freq表示并发写入次数，总写入数据量为batchSize*freq

若不传入参数，默认batchSize=2000，freq=50，serverIP="localhost"，serverPort==8848

### 4. 流数据写入和订阅例子
Java API提供了ThreadedClient、ThreadPooledClient和PollingClient三种订阅模式订阅流表的数据。三种模式的主要区别在于收取数据的方式。ThreadedClient单线程执行，并且对收到的消息直接执行用户定义的handler函数进行处理；ThreadPooledClient多线程执行，对收到的消息进行多线程并行调用用户定义的handler函数进行处理；PollingClient返回一个消息队列，用户可以通过轮寻的方式获取和处理数据。在本例流数据订阅的源代码中，可以选择用ThreadedClient类，handler处理数据，或者使用PollingClient获取和处理数据。

#### 4.1 代码说明
本例实现了流数据表的写入和流数据订阅的功能，会订阅服务端发布的数据并在Java应用端打印出来，主要有4个函数和类如下：
* writeStreamTable函数，用于创建流数据表，以及用run函数运行tableInsert脚本把模拟数据写入流表，并显示写入的结果。
* createBasicTable函数，定义待写入的数据，该函数创建了一个本地的表对象BasicTable。
* pollingClient类，用PollingClient订阅模式订阅流表的数据，并将获取的数据展示出来。
* ThreadedClient类，用ThreadedClient订阅模式订阅流表的数据。
* SampleMessageHandler类，处理ThreadedClient订阅的流表数据，将获取的数据展示出来
* main函数，与DolphinDB server建立连接，选择订阅模式。

#### 4.2 运行
将代码打包成xxx.jar 执行 
```
java -jar xxx.jar [serverIP] [serverPort] [subscribePort] [subscribeMethod]
```
subscribeMethod有2个选项：
* 'T'，用ThreadedClient开启订阅。
* 'P'，用PollingClient开启订阅。

若不传入参数，默认serverIP="localhost"，serverPort==8848，subscribePort=8892，subscribeMethod='P'.

程序运行之后，处于等待流数据的状态，在DolphinDB 服务端运行以下脚本发布数据：
```
for(x in 0:20000){
    time =time(now())
    sym= rand(`S`M`MS`GO, 1)
    qty= rand(1000..2000,1)
    price = rand(2335.34,1)
    insert into Trades values(x, time,sym, qty, price)
}
insert into Trades values(-1, time,sym, qty, price)
```