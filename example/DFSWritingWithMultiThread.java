package com.dolphindb;

import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.data.Vector;

import javax.net.ssl.HostnameVerifier;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;

public class DFSWritingWithMultiThread {

    static Hashtable<String, DBTaskItem> tables;

    static Hashtable<String, List<Integer>> groups ;
    static String HOST ;
    static int PORT ;
    static String DBPATH ;
    static String TBNAME;
    static int FREQ ;
    static int BATCHSIZE;

    static int insertRowCount =0 ;
    public static void main(String[] args) throws Exception{
        //记录哈希值分组，每组对应一个线程
        groups = new Hashtable<>();
        groups.put("group1", Arrays.asList(0,1,2));
        groups.put("group2", Arrays.asList(3,4,5));
        groups.put("group3", Arrays.asList(6,7));
        groups.put("group4", Arrays.asList(8,9));

        tables = new Hashtable<>();
        if(args.length==0)
        {
            BATCHSIZE=2000;
            FREQ=50;
            HOST = "localhost"; //Your DolphinDB server HOST
            PORT = 8848; //Your DolphinDB server PORT
        }
        else if(args.length!=4) {
            try {
                FREQ = Integer.parseInt(args[0]);
                BATCHSIZE = Integer.parseInt(args[1]);
                HOST = args[2];
                PORT = Integer.parseInt(args[3]);
            }catch (Exception e)
            {
                System.out.println("Wrong arguments");
            }
        }
        else
        {
            System.out.println("Wrong arguments");
            return;
        }
        DBPATH = "dfs://DolphinDBUUID";
        TBNAME = "device_status";
        new com.dolphindb.DFSWritingWithMultiThread().generateData(tables,BATCHSIZE,FREQ);
    }

    private BasicTable createBasicTable(int n) throws Exception{
        List<String> colNames = new ArrayList<String>();
        colNames.add("time");
        colNames.add("areaId");
        colNames.add("deviceId");
        colNames.add("value");

        List<Vector> cols = new ArrayList<Vector>(){};

        //Timnstamp
        BasicTimestampVector timestamp = new BasicTimestampVector(n);
        BasicUuidVector areaId = new BasicUuidVector(n);
        BasicUuidVector deviceId = new BasicUuidVector(n);
        BasicDoubleVector value = new BasicDoubleVector(n);

        cols.add(timestamp);
        cols.add(areaId);
        cols.add(deviceId);
        cols.add(value);

        return new BasicTable(colNames, cols);
    }

    public void generateData(Hashtable<String,DBTaskItem> tbs, int batchSize, int freq) throws Exception{
        //定义每组的数据队列
        for(String key:groups.keySet()){
            tables.put(key , new DBTaskItem(null,null));
        }
        //预生成areaId和deviceId的UUID库
        List<BasicUuid> areas = new ArrayList<>();
        for(int t=0;t<100;t++){
            areas.add(BasicUuid.fromString(getRandomUUID()));
        }
        List<BasicUuid> devices = new ArrayList<>();
        for(int t=0;t<100;t++){
            devices.add(BasicUuid.fromString(getRandomUUID()));
        }
        //开启写DolphinDB消费线程
        new Thread(new TaskConsumer()).start();
        //循环生成客户端数据并加入消费队列
        long st = System.currentTimeMillis();
        for(int j=0;j<batchSize * freq;j++){
            generateOneRow(tbs, areas,devices, ROWS);
        }
        long ed = System.currentTimeMillis();
        System.out.println("create data cost : " + (ed-st));

    }

    public String getGroupId(int hashCode){
        for (String key : groups.keySet()) {
            List<Integer> hashCodeList =  groups.get(key);
            if(hashCodeList.contains(hashCode))
                return key;
        }
        return null;
    }
    //对客户端写入一行
    public void generateOneRow(Hashtable<String,DBTaskItem> tbs, List<BasicUuid> areas, List<BasicUuid> devices, int n) throws Exception{
        Random rand = new Random();
        BasicUuid areaUUID = areas.get(rand.nextInt(areas.size()));
        BasicUuid deviceUUID = devices.get(rand.nextInt(devices.size()));
        int key = areaUUID.hashBucket(10);
        String groupId = getGroupId(key);

        if(tbs.containsKey(groupId)){
            if(tbs.get(groupId).currentTable==null){
                tbs.get(groupId).currentTable = new BasicTableEx(createBasicTable(n));
            }
        }else{
            throw new Exception("group out of range");
        }

        if(tbs.get(groupId).currentTable.add(LocalDateTime.of(2020,1,1,1,1,1,1), areaUUID,deviceUUID, (double)(50*rand.nextDouble()))){
            if(tbs.get(groupId).currentTable.isFull()){
                tbs.get(groupId).pushToQueue();
            }
        }
    }

    public static String getRandomUUID(){
        UUID uuid=UUID.randomUUID();
        return uuid.toString();
    }

    public class TaskConsumer implements Runnable {
        public void run(){
            for(String key:groups.keySet()){
                new Thread(new DDBProxy(key)).start();
            }
        }
    }

    public class DDBProxy implements Runnable {
        String _key = null;
        public DDBProxy(String key){
            _key = key;
        }
        public void run(){
            do {
                if (tables.get(_key).TaskQueue.size() > 0) {
                    BasicTable data = tables.get(_key).TaskQueue.poll();
                    saveDataToDDB(data);
                    insertRowCount += data.rows();
                    if(insertRowCount<2000 || insertRowCount>90000)
                        System.out.println(String.format("insertRowCount = %s ; now is %s ", insertRowCount ,  LocalDateTime.now()));
                }
            } while (true);
        }
    }

    public static void saveDataToDDB(BasicTable data){
        DBConnection conn = new DBConnection();
        try {
            conn.connect(HOST,  PORT,String.format("def saveData(data){ loadTable('%s','%s').tableInsert(data)}", DBPATH,TBNAME));
            conn.login("admin","123456",false);
            List<Entity> arg = new ArrayList<Entity>(1);
            arg.add(data);
            long st = System.currentTimeMillis();
            conn.run("saveData", arg);
            long ed = System.currentTimeMillis();
            System.out.println(String.format("insert %s rows, cost %s ms", data.rows(), ed-st));

        }catch (IOException ex){
            System.out.println(data.getColumn(1).getString());
            ex.printStackTrace();
        }
        conn.close();
    }

    public class BasicTableEx {
        int _currentAddIndex = 0;
        BasicTable _bt = null;
        public BasicTableEx(BasicTable table){
            _bt = table;
        }

        public boolean add(LocalDateTime timestamp, BasicUuid areaId, BasicUuid deviceId, double value) throws Exception{
            if(_currentAddIndex>=_bt.rows()) return false;
            ((BasicTimestampVector)_bt.getColumn(0)).setTimestamp(_currentAddIndex,timestamp);
            ((BasicUuidVector)_bt.getColumn(1)).set(_currentAddIndex,areaId);
            ((BasicUuidVector)_bt.getColumn(2)).set(_currentAddIndex,deviceId);
            ((BasicDoubleVector)_bt.getColumn(3)).setDouble(_currentAddIndex,value);
            _currentAddIndex ++;
            return  true;
        }

        public boolean isFull(){
            return _bt.rows()==_currentAddIndex;
        }
    }

    public class DBTaskItem{
        public BasicTableEx currentTable;
        public ConcurrentLinkedQueue<BasicTable> TaskQueue = new ConcurrentLinkedQueue<>();
        private Thread _thread ;

        public DBTaskItem(BasicTableEx currTable , Thread thread){
            currentTable = currTable;
            this._thread = thread;
        }

        public void pushToQueue() {
            TaskQueue.add(currentTable._bt);
            // System.out.println("TaskQueue size = " + TaskQueue.size());
            currentTable = null;
        }
    }
}
