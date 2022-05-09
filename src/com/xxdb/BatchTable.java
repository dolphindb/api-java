package com.xxdb;

import com.xxdb.data.*;
import com.xxdb.data.Vector;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class BatchTable extends Thread{

    /**
     * 两个参数的数据类型
     */
    private static final HashSet<String> set = new HashSet<String>();
    static{
        set.add("UUID");
        set.add("COMPLEX");
        set.add("IPADDR");
        set.add("INT128");
        set.add("POINT");
        set.add("DURATION");
    }
    /**
     * 数据库名称
     */
    private String databaseName;
    /**
     * 表名
     */
    private String tableName;
    /**
     * 批大小
     */
    private int batchSize;
    /**
     * 列名
     */
    private List<String> colNames;
    /**
     * 列类型
     */
    private List<String> colTypes;
    /**
     * 默认批大小
     */
    private static int BATCH_SIZE = 1000;
    /**
     * 队列大小
     */
    private static int CAPACITY = 10000000;
    /**
     * 任务队列
     */
    public LinkedBlockingQueue<Object[]> linkedBlockingQueue = new LinkedBlockingQueue<>(CAPACITY);
    /**
     * DolphinDB连接对象
     */
    private static DBConnection conn = new DBConnection();
    /**
     * 数据表对象
     */
    public static BasicTable db;


    public BatchTable(DBConnection conn, String databaseName, String tableName) {
        this(conn, databaseName,tableName,BATCH_SIZE);
    }

    public BatchTable(DBConnection c, String databaseName, String tableName, int batchSize) {
        conn = c;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.batchSize = batchSize;
        this.init();
    }

    public void init() {

        if(databaseName == null || databaseName.length() == 0){
            try {
                conn.run(String.format("def saveMemData%s(data){ %s.tableInsert(data)}",this.tableName,this.tableName));
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.colNames = this.getColNames();
            this.colTypes = this.getColTypes();
        }
        else {
            try {
                db = (BasicTable) conn.run(String.format("loadTable('%s','%s')", this.databaseName, this.tableName));
                conn.run(String.format("def saveDFSData%s%s(data){ loadTable('%s','%s').tableInsert(data)}", this.databaseName.substring(6),this.tableName,this.databaseName,this.tableName));
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.colNames = this.getColNames(db);
            this.colTypes = this.getColTypes(db);
        }
        this.start();
    }



    @Override
    public void run() {
        List<Vector> colsBatch = getCols(batchSize);
        List<Vector> cols = new ArrayList<Vector>();
        String type;
        Vector col;
        while (true) {
            if (!linkedBlockingQueue.isEmpty()) {
                try {
                    int flag = Math.min(batchSize, linkedBlockingQueue.size());
                    if (flag == batchSize)
                        cols = colsBatch;
                    else {
                        cols = getCols(flag);
                    }
                    for (int i = 0; i < flag; i++) {
                        Object[] objects = new Object[0];
                        try {
                            objects = linkedBlockingQueue.take();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        for (int cindex = 0, index = 0; cindex < cols.size(); cindex++,index++) {
                            col = cols.get(cindex);
                            // 判断是否是最后一列ReceivedTime
                            if (cindex == cols.size() - 1){
                                col.set(i,new BasicTimestamp(Utils.parseTimestamp(System.currentTimeMillis())));
                                break;
                            }
                            type = colTypes.get(cindex);
                            if (set.contains(type))
                                col.set(i, getScalar(type, objects[index], objects[++index]));
                            else
                                col.set(i, getScalar(type, objects[index]));
                        }
                    }
                    BasicTable basicTable = new BasicTable(colNames, cols);
                    List<Entity> arguments = new ArrayList<Entity>(1);
                    arguments.add(basicTable);
                    if (this.databaseName == null || this.databaseName.length() == 0){
                        conn.run(String.format("saveMemData%s",this.tableName),arguments);
                    }else {
                        conn.run(String.format("saveDFSData%s%s",this.databaseName.substring(6),this.tableName),arguments);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
    }


    public void insert(Object...objects){
        try {
            linkedBlockingQueue.put(objects);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public List<Vector> getCols(int size){
        List<Vector> cols = new ArrayList<>();
        for (String type : colTypes) {
            Vector vector = null;
            switch (type) {
                case "CHAR":
                case "INT":
                    vector = new BasicIntVector(size);
                    break;
                case "STRING":
                case "SYMBOL":
                    vector = new BasicStringVector(size);
                    break;
                case "BOOL":
                    vector = new BasicBooleanVector(size);
                    break;
                case "SHORT":
                    vector = new BasicShortVector(size);
                    break;
                case "LONG":
                    vector = new BasicLongVector(size);
                    break;
                case "DATE":
                    vector = new BasicDateVector(size);
                    break;
                case "MONTH":
                    vector = new BasicMonthVector(size);
                    break;
                case "TIME":
                    vector = new BasicTimeVector(size);
                    break;
                case "MINUTE":
                    vector = new BasicMinuteVector(size);
                    break;
                case "SECOND":
                    vector = new BasicSecondVector(size);
                    break;
                case "DATETIME":
                    vector = new BasicDateTimeVector(size);
                    break;
                case "TIMESTAMP":
                    vector = new BasicTimestampVector(size);
                    break;
                case "NANOTIME":
                    vector = new BasicNanoTimeVector(size);
                    break;
                case "NANOTIMESTAMP":
                    vector = new BasicNanoTimestampVector(size);
                    break;
                case "FLOAT":
                    vector = new BasicFloatVector(size);
                    break;
                case "DOUBLE":
                    vector = new BasicDoubleVector(size);
                    break;
                case "UUID":
                    vector = new BasicUuidVector(size);
                    break;
                case "COMPLEX":
                    vector = new BasicComplexVector(size);
                    break;
                case "DATEHOUR":
                    vector = new BasicDateHourVector(size);
                    break;
                case "IPADDR":
                    vector = new BasicIPAddrVector(size);
                    break;
                case "INT128":
                    vector = new BasicInt128Vector(size);
                    break;
                case "POINT":
                    vector = new BasicPointVector(size);
                    break;
                case "DURATION":
                    vector = new BasicDurationVector(size);
                    break;
            }
            cols.add(vector);
        }
        return cols;
    }

    public Scalar getScalar(String type,Object object){
        Scalar entity = null;
        switch (type) {
            case "CHAR":
            case "INT":
                entity = new BasicInt((int) object); break;
            case "STRING":
            case "SYMBOL":
                entity = new BasicString((String)object);break;
            case "BOOL":
                entity = new BasicBoolean((boolean)object);break;
            case "SHORT":
                entity = new BasicShort(Short.parseShort(object.toString()));break;
            case "LONG":
                entity = new BasicLong((long)object);break;
            case "DATE":
                entity = new BasicDate((LocalDate) object);break;
            case "MONTH":
                entity = new BasicMonth((YearMonth) object);break;
            case "TIME":
                entity = new BasicTime((LocalTime) object);break;
            case "MINUTE":
                entity = new BasicMinute((LocalTime)object);break;
            case "SECOND":
                entity = new BasicSecond((LocalTime) object);break;
            case "DATETIME":
                entity = new BasicDateTime((LocalDateTime) object);break;
            case "TIMESTAMP":
                entity = new BasicTimestamp((LocalDateTime)object);break;
            case "NANOTIME":
                entity = new BasicNanoTime((LocalTime)object);break;
            case "NANOTIMESTAMP":
                entity = new BasicNanoTimestamp((LocalDateTime)object);break;
            case "FLOAT":
                entity = new BasicFloat((float)object);break;
            case "DOUBLE":
                entity = new BasicDouble((double)object);break;
            case "DATEHOUR":
                entity = new BasicDateHour((LocalDateTime) object);break;
        }
        return entity;
    }
    public Scalar getScalar(String type, Object object,Object next){
        Scalar entity = null;
        switch (type) {
            case "IPADDR":
                entity = new BasicIPAddr((long)object,(long) next);break;
            case "INT128":
                entity = new BasicInt128((long)object,(long) next);break;
            case "POINT":
                entity = new BasicPoint((double)object,(double) next);break;
            case "DURATION":
                entity = new BasicDuration((Entity.DURATION) object,(int) next);break;
            case "UUID":
                entity = new BasicUuid((long)object,(long) next);break;
            case "COMPLEX":
                entity = new BasicComplex((double)object,(double) next);break;
        }
        return entity;
    }

    // 获取colNames：分布式表
    public List<String> getColNames(BasicTable db){
        List<Entity> arguments1 = new ArrayList<Entity>(1);
        arguments1.add(db);
        BasicStringVector basicStringVector = null;
        try {
            basicStringVector = (BasicStringVector)conn.run("columnNames",arguments1);
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<String> list = new ArrayList<>();
        if (basicStringVector != null) {
            for (int i = 0; i < basicStringVector.rows(); i++) {
                list.add(basicStringVector.getString(i));
            }
        }
        return list;
    }

    // 获取colNames: 流数据表
    public List<String> getColNames(){
        BasicStringVector basicStringVector = null;
        try {
            basicStringVector = (BasicStringVector)conn.run(String.format("columnNames(%s)",this.tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<String> list = new ArrayList<>();
        if (basicStringVector != null) {
            for (int i = 0; i < basicStringVector.rows(); i++) {
                list.add(basicStringVector.getString(i));
            }
        }
        return list;
    }

    // 获取colTypes
    public List<String> getColTypes(BasicTable db){

        List<Entity> arguments1 = new ArrayList<Entity>(1);
        arguments1.add(db);
        BasicDictionary o1 = null;
        try {
            o1 = (BasicDictionary)conn.run("schema",arguments1);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BasicTable table = null;
        if (o1 != null) {
            table = (BasicTable) o1.get(new BasicString("colDefs"));
        }
        BasicStringVector str = null;
        if (table != null) {
            str = (BasicStringVector) table.getColumn(1);
        }
        ArrayList<String> list = new ArrayList<>();
        if (str != null) {
            for (int i = 0; i < str.rows(); i++) {
                list.add(str.get(i).getString());
            }
        }
        return list;
    }

    // 获取colTypes，流数据表
    public List<String> getColTypes(){
        BasicDictionary o1 = null;
        try {
            o1 = (BasicDictionary)conn.run(String.format("schema(%s)",this.tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        BasicTable table = null;
        if (o1 != null) {
            table = (BasicTable) o1.get(new BasicString("colDefs"));
        }
        BasicStringVector str = null;
        if (table != null) {
            str = (BasicStringVector) table.getColumn(1);
        }
        ArrayList<String> list = new ArrayList<>();
        if (str != null) {
            for (int i = 0; i < str.rows(); i++) {
                list.add(str.get(i).getString());
            }
        }
        return list;
    }


    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

}
