package com.xxdb.multithreadtablewriter;

import com.xxdb.DBConnection;
import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.data.*;
import com.xxdb.route.Domain;
import com.xxdb.route.DomainFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class MultithreadTableWriter {
    private Logger logger_=Logger.getLogger(getClass().getName());
    public static class ThreadStatus{
        long threadId;
        long lastErrorTime;
        ErrorCodeInfo errorCodeInfo;
        int unWriteCount;
        long sentRows;
    };
    static class WriterThread implements Runnable{
        WriterThread(MultithreadTableWriter tableWriter,DBConnection conn) {
            tableWriter_=tableWriter;
            lastErrorTime_=0;
            sentRows_=0;
            conn_ = conn;
            writeThread_ = new Thread(this);
            writeThread_.start();
        }
        @Override
        public void run(){
            boolean exit = false;
            List<List<Entity>> items=new ArrayList<>();
            while (isExit() == false && exit == false ) {
                if (conn_==null) {// connection lost
                    if (tableWriter_.autoReconnect_ == false) {//don't need reconnection
                        tableWriter_.setExit(errorCodeInfo_);
                        break;
                    }
                    try {
                        Thread.sleep (100);
                        conn_ = tableWriter_.newConn();
                    } catch (Exception e){
                        tableWriter_.logger_.warning("threadid="+writeThread_.getId()+ " init connection error: "+ e);
                        setError(ErrorCodeInfo.Code.EC_Server, "Init connection error: " + e);
                        conn_ = null;
                        //std::cerr << Util::createTimestamp(Util::getEpochTime())->getString() << " Backgroud thread of table (" << tableWriter_.dbName_ << " " << tableWriter_.tableName_ << "). Failed to send data to server, with exception: " << e.what() << std::endl;
                    }
                    if (conn_==null) {
                        continue;
                    }
                }
                if (init() == false) {//init
                    continue;
                }
                while (isExit() == false && exit == false && conn_!=null) {
                    items.clear();
                    try {
                        if (failedQueue_.size() < 1) {//没有失败待重发的数据
                            synchronized (writeQueue_) {
                                if(writeQueue_.size()<tableWriter_.batchSize_){
                                    writeQueue_.wait(tableWriter_.throttleMilsecond_);
                                }
                                if (writeQueue_.isEmpty()) {
                                    continue;
                                }
                                for(List<Entity> row:writeQueue_){
                                    if(row!=null) {
                                        items.add(row);
                                    }else{
                                        exit = true;
                                        break;
                                    }
                                }
                                writeQueue_.clear();
                            }
                        } else {//先发失败的
                            synchronized (failedQueue_) {
                                items.addAll(failedQueue_);
                                failedQueue_.clear();
                            }
                            Thread.sleep(100);
                        }
                    }catch (Exception e){
                        break;
                    }
                    int size = items.size();
                    if (size < 1)
                        continue;
                    //std::cout << "writeTableAllData size:" << size << std::endl;
                    tableWriter_.logger_.info (" writeAllData="+size);
                    boolean isWriteDone = true;
                    BasicTable writeTable=null;
                    try {//create table
                        //RECORDTIME("MTTW:createTable");
                        tableWriter_.logger_.info(" createTable="+size);
                        List<Vector> columns=new ArrayList<>();
                        for(Entity.DATA_TYPE one:tableWriter_.colTypes_){
                            columns.add(BasicEntityFactory.instance().createVectorWithDefaultValue(one,size));
                        }
                        writeTable=new BasicTable(tableWriter_.colNames_, columns);
                        int rowindex=0,colindex;
                        for (List<Entity> row:items) {
                            colindex=0;
                            try {
                                for (Vector col : columns) {
                                    Scalar scalar = (Scalar) row.get(rowindex);
                                    if (scalar != null)
                                        col.set(colindex, scalar);
                                    else
                                        col.setNull(colindex);
                                    colindex++;
                                }
                            }catch (Exception e){
                                isWriteDone = false;
                                tableWriter_.logger_.warning("threadid="+ writeThread_.getId()+ " sendindex="+ sentRows_+ " Append row failed: "+ e);
                                setError(ErrorCodeInfo.Code.EC_InvalidObject, "Append row failed: " + e);
                                break;
                            }
                        }
                    } catch (Exception e){
                        tableWriter_.logger_.warning("threadid="+ writeThread_.getId()+ " Create table error: "+ e);
                        setError(ErrorCodeInfo.Code.EC_Server, "Create table error: " + e);
                        isWriteDone = false;
                        //std::cerr << Util::createTimestamp(Util::getEpochTime())->getString() << " Backgroud thread of table (" << tableWriter_.dbName_ << " " << tableWriter_.tableName_ << "). Failed to send data to server, with exception: " << e.what() << std::endl;
                    }
                    if (isWriteDone && writeTable!=null) {//may contain empty vector for exit
                        String runscript="";
                        try {//save table
                            //RECORDTIME("MTTW:saveTable");
                            tableWriter_.logger_.info(" saveTable="+size);
                            List<Entity> args=new ArrayList<>();
                            args.add(writeTable);
                            runscript = scriptTableInsert_;
                            conn_.run(runscript, args);
                            if (scriptSaveTable_.isEmpty() == false) {
                                runscript = scriptSaveTable_;
                                conn_.run(runscript);
                            }
                            sentRows_ += items.size();
                        } catch (Exception e){
                            tableWriter_.logger_.warning("threadid="+writeThread_.getId()+" sendindex="+sentRows_+" Save table error: "+e+" script:"+runscript);
                            setError(ErrorCodeInfo.Code.EC_Server,"Save table error: "+ e + " script: " + runscript);
                            conn_ = null;
                            isWriteDone = false;
                            //std::cerr << Util::createTimestamp(Util::getEpochTime())->getString() << " Backgroud thread of table (" << tableWriter_.dbName_ << " " << tableWriter_.tableName_ << "). Failed to send data to server, with exception: " << e.what() << std::endl;
                        }
                    }
                    tableWriter_.logger_.info(" done="+isWriteDone);
                    if (isWriteDone == false) {
                        synchronized (failedQueue_){
                            failedQueue_.addAll(items);
                        }
                    }
                }
            }
        }
        boolean init(){
            if (tableWriter_.tableName_.isEmpty()) {
                scriptTableInsert_ = "tableInsert{\"" + tableWriter_.dbName_ + "\"}";
            }
            else if (tableWriter_.isPartionedTable_) {
                scriptTableInsert_ = "tableInsert{loadTable(\"" + tableWriter_.dbName_ + "\",\"" + tableWriter_.tableName_ + "\")}";
            }
            else {
                {
                    String tempTableName = "tmp" + tableWriter_.tableName_;
                    String colNames="";
                    String colTypes="";
                    for (int i = 0; i < tableWriter_.colNames_.size(); i++) {
                        colNames += "`" + tableWriter_.colNames_.get(i);
                        colTypes += "`" + tableWriter_.colTypeString_.get(i);
                    }
                    String scriptCreateTmpTable = "tempTable = table(" + "1000:0," + colNames + "," + colTypes + ")";
                    try {
                        conn_.run(scriptCreateTmpTable);
                    }catch (Exception e) {
                        tableWriter_.logger_.warning("threadid=" + writeThread_.getId()+ " Init table error: "+e+ " script:"+scriptCreateTmpTable);
                        setError(ErrorCodeInfo.Code.EC_Server, "Init table error: " + e + " script: " + scriptCreateTmpTable);
                        conn_ = null;
                        //std::cerr << Util::createTimestamp(Util::getEpochTime())->getString() << " Backgroud thread of table (" << tableWriter_.dbName_ << " " << tableWriter_.tableName_ << "). Failed to init data to server, with exception: " << e.what() << std::endl;
                        return false;
                    }
                }
                scriptTableInsert_ = "tableInsert{tempTable}";
                scriptSaveTable_ = "saveTable(database(\"" + tableWriter_.dbName_ + "\")" + ",tempTable,\"" + tableWriter_.tableName_ + "\", 1);tempTable.clear!();";
            }
            return true;
        }
        void setError(ErrorCodeInfo.Code code, String info){
            lastErrorTime_ = System.currentTimeMillis();
            errorCodeInfo_ = new ErrorCodeInfo(code, info);
            if (tableWriter_.autoReconnect_ == false) {
                tableWriter_.setExit(errorCodeInfo_);
            }
        }
        boolean isExit() { return tableWriter_.exit_; }
        void getStatus(ThreadStatus status){
            status.errorCodeInfo = errorCodeInfo_;
            status.lastErrorTime = lastErrorTime_;
            status.sentRows = sentRows_;
            status.threadId = writeThread_.getId();
            status.unWriteCount = writeQueue_.size() + failedQueue_.size();
        }
        void exit(){
            synchronized (writeQueue_) {
                writeQueue_.add(null);
            }
        }
        MultithreadTableWriter tableWriter_;
        DBConnection conn_;

        String scriptTableInsert_,scriptSaveTable_;

        List<List<Entity>> writeQueue_=new ArrayList<>();
        List<List<Entity>> failedQueue_=new ArrayList<>();
        Thread writeThread_;
        long sentRows_;

        long lastErrorTime_;
        ErrorCodeInfo errorCodeInfo_;
    };
    private String hostName_;
    private int port_;
    private String userId_;
    private String password_;
    private boolean useSSL_;
    private String dbName_;
    private String tableName_;
    private int batchSize_;
    private int throttleMilsecond_;
    private String initialScript_;
	private boolean autoReconnect_;
	private boolean compress_;
	private boolean highAvailability_;
    private int keepAliveTime_;
    private boolean isPartionedTable_, exit_;
    private List<String> colNames_=new ArrayList<>(),colTypeString_=new ArrayList<>();
    private List<Entity.DATA_TYPE> colTypes_=new ArrayList<>();
    private Domain partitionDomain_;
    private int partitionColumnIdx_;
    private int threadByColIndexForNonPartion_;
    private List<WriterThread> threads_=new ArrayList<>();
    private String[] highAvailabilitySites_=null;
    private ErrorCodeInfo errorCodeInfo_;
    public MultithreadTableWriter(String hostName, int port, String userId, String password,
                                  String dbName, String tableName, boolean useSSL, int batchSize, float throttle,
                                  int threadCount, String threadByColName,String initialScript,
                                  boolean autoReconnect, boolean compress) throws Exception{
        hostName_=hostName;
        port_=port;
        userId_=userId;
        password_=password;
        useSSL_=useSSL;
        dbName_=dbName;
        tableName_=tableName;
        batchSize_=batchSize;
        throttleMilsecond_=(int)throttle*1000;
        exit_=false;
        initialScript_=initialScript;
        autoReconnect_=autoReconnect;
        compress_=compress;
        keepAliveTime_=30;
        highAvailability_=false;
        if(threadCount < 1){
            throw new RuntimeException("threadCount must be greater than 1.");
        }
        if(batchSize < 0){
            throw new RuntimeException("batchSize must be greater than 1.");
        }
        if (batchSize_ < 1)
            batchSize_ = 1;
        if(throttleMilsecond_ < 1)
            throttleMilsecond_ = 1;
        if(throttle < 0){
            throw new RuntimeException("throttle must be greater than 0.");
        }
        DBConnection pConn = newConn();
        if(pConn==null){
            throw new RuntimeException("Failed to connect to server.");
        }

        BasicDictionary schema;
        if(tableName.isEmpty()){
            schema = (BasicDictionary)pConn.run("schema(" + dbName + ")");
        }else{
            schema = (BasicDictionary)pConn.run("schema(loadTable(\"" + dbName + "\",\"" + tableName + "\"))");
        }
        Entity partColNames = schema.get(new BasicString("partitionColumnName"));
        if(partColNames==null){//partitioned table
            isPartionedTable_ = true;
        }else{//没有分区
            if(tableName.isEmpty() == false){//文件表
                if(threadCount > 1){
                    throw new RuntimeException("Non-partioned table support single thread only.");
                }
            }
            isPartionedTable_ = false;
            if(threadByColName.isEmpty()){
                if(threadCount > 1){//只有多线程的时候需要
                    throw new RuntimeException("threadByColNameForNonPartioned must be specified for non-partioned table.");
                }
            }
        }

        BasicTable colDefs = (BasicTable)schema.get(new BasicString("colDefs"));
        BasicIntVector colDefsTypeInt = (BasicIntVector)colDefs.getColumn("typeInt");
        int columnSize = colDefs.rows();

        BasicStringVector colDefsName = (BasicStringVector)colDefs.getColumn("name");
        BasicStringVector colDefsTypeString = (BasicStringVector)colDefs.getColumn("typeString");
        for(int i = 0; i < columnSize; i++){
            colNames_.add(colDefsName.getString(i));
            colTypes_.add(Entity.DATA_TYPE.valueOf(colDefsTypeInt.getInt(i)));
            colTypeString_.add(colDefsTypeString.getString(i));
        }
        if(isPartionedTable_){
            Entity partitionSchema;
            int partitionType;
            if(partColNames.isScalar()){
                partitionColumnIdx_ = ((BasicInt)schema.get(new BasicString("partitionColumnIndex"))).getInt();
                partitionSchema = schema.get(new BasicString("partitionSchema"));
                partitionType =  ((BasicInt)schema.get(new BasicString("partitionType"))).getInt();
            }else{
                BasicStringVector partColNamesVec = (BasicStringVector)partColNames;
                int dims = partColNamesVec.rows();
                if(dims > 1 && threadByColName.isEmpty()){
                    throw new RuntimeException("Please specify threadByColName for this partition table.");
                }
                int index = -1;
                for(int i=0; i<dims; ++i){
                    if(partColNamesVec.getString(i).equalsIgnoreCase(threadByColName)){
                        index = i;
                        break;
                    }
                }
                if(index < 0)
                    throw new RuntimeException("Can't find specified partition column name.");
                partitionColumnIdx_ = ((BasicIntVector)schema.get(new BasicString("partitionColumnIndex"))).getInt(index);
                partitionSchema = ((BasicAnyVector)schema.get(new BasicString("partitionSchema"))).get(index);
                partitionType =  ((BasicIntVector)schema.get(new BasicString("partitionType"))).getInt(index);
            }
            Entity.DATA_TYPE dataColType = colTypes_.get(partitionColumnIdx_);
            Entity.PARTITION_TYPE partitionColtype=Entity.PARTITION_TYPE.values()[partitionType];
            partitionDomain_ = DomainFactory.createDomain(partitionColtype, dataColType, partitionSchema);
        } else {//isPartionedTable_==false
            if(threadByColName.isEmpty() == false){
                int threadcolindex = -1;
                for(int i=0; i<colNames_.size(); i++){
                    if(colNames_.get(i).equalsIgnoreCase(threadByColName)){
                        threadcolindex=i;
                        break;
                    }
                }
                if(threadcolindex < 0){
                    throw new RuntimeException("Can't find column name for "+threadByColName);
                }
                threadByColIndexForNonPartion_=threadcolindex;
            }
        }
        // init done, start thread now.
        for(int i = 0; i < threadCount; i++){
            if (pConn == null) {
                pConn = newConn();
            }
            WriterThread writerThread = new WriterThread(this,pConn);
            threads_.add(writerThread);
            pConn = null;
        }
    }
    public void getUnwrittenData(List<List<Entity>> unwrite){
        for(WriterThread writeThread : threads_){
            synchronized (writeThread.failedQueue_) {
                unwrite.addAll(writeThread.failedQueue_);
            }
            synchronized (writeThread.writeQueue_){
                unwrite.addAll(writeThread.writeQueue_);
            }
        }
    }
    public void getStatus(List<ThreadStatus> statusList){
        for(WriterThread writeThread : threads_){
            ThreadStatus status=new ThreadStatus();
            writeThread.getStatus(status);
            statusList.add(status);
        }
    }
    public void waitExit() throws InterruptedException{
        for(WriterThread one:threads_){
            one.exit();
            one.writeThread_.wait();
            one.conn_=null;
        }
        threads_.clear();
    }
    public void exit(){
        setExit(new ErrorCodeInfo(ErrorCodeInfo.Code.EC_UserBreak, "User exit"));
    }
    public boolean insert(List<List<Entity>> vectorOfVector,ErrorCodeInfo pErrorInfo){
        if(threads_.size() > 1){
            if(isPartionedTable_){
                Vector pvector=BasicEntityFactory.instance().createVectorWithDefaultValue(colTypes_.get(partitionColumnIdx_),vectorOfVector.size());
                int rowindex=0;
                try {
                    for (List<Entity> row : vectorOfVector) {
                        if (row.size() != colTypes_.size()) {
                            pErrorInfo.set(ErrorCodeInfo.Code.EC_InvalidParameter, "Vector in vectorOfVector size " + row.size() +
                                    " mismatch " + colTypes_.size());
                            return false;
                        }
                        if (row.get(partitionColumnIdx_) != null) {
                            Scalar scalar=(Scalar) row.get(partitionColumnIdx_);
                            if(scalar!=null)
                                pvector.set(rowindex, scalar);
                            else
                                pvector.setNull(rowindex);
                        } else {
                            pvector.setNull(rowindex);
                        }
                        rowindex++;
                    }
                }catch (Exception e){
                    pErrorInfo.set(ErrorCodeInfo.Code.EC_InvalidParameter, "Row in vectorOfVector " + rowindex +
                            " mismatch type " + colTypes_.get(partitionColumnIdx_));
                    return false;
                }
                List<Integer> threadindexes = partitionDomain_.getPartitionKeys(pvector);
                for(int row = 0; row < threadindexes.size(); row++){
                    if(insertThreadWrite(threadindexes.get(row), vectorOfVector.get(row), pErrorInfo) == false){
                        return false;
                    }
                }
            }else{
                Vector partionvector=BasicEntityFactory.instance().createVectorWithDefaultValue(colTypes_.get(threadByColIndexForNonPartion_),vectorOfVector.size());
                int rowindex=0;
                try{
                    for(List<Entity> row : vectorOfVector) {
                        if (row.size() != colTypes_.size()) {
                            pErrorInfo.set(ErrorCodeInfo.Code.EC_InvalidParameter, "Vector in vectorOfVector size " + row.size() +
                                    " mismatch " + colTypes_.size());
                            return false;
                        }
                        Scalar scalar=(Scalar) row.get(threadByColIndexForNonPartion_);
                        if(scalar!=null)
                            partionvector.set(rowindex, scalar);
                        else
                            partionvector.setNull(rowindex);
                        rowindex++;
                    }
                }catch (Exception e){
                    pErrorInfo.set(ErrorCodeInfo.Code.EC_InvalidParameter, "Row in vectorOfVector " + rowindex +
                            " mismatch type " + colTypes_.get(partitionColumnIdx_));
                    return false;
                }
                int threadindex;
                for(rowindex=0;rowindex<vectorOfVector.size();rowindex++){
                    threadindex=partionvector.hashBucket(rowindex,threads_.size());
                    if(insertThreadWrite(threadindex, vectorOfVector.get(rowindex), pErrorInfo) == false){
                        return false;
                    }
                }
            }
        }else{
            for(List<Entity> row : vectorOfVector){
                if(insertThreadWrite(0, row, pErrorInfo) == false){
                    return false;
                }
            }
        }
        return true;
    }
    private boolean insertThreadWrite(int threadhashkey, List<Entity> row, ErrorCodeInfo pErrorInfo){
        if(threadhashkey < 0){
            logger_.warning("add invalid hash="+threadhashkey);
            threadhashkey = 0;
            //pErrorInfo->set(ErrorCodeInfo::EC_InvalidColumnType, "Failed to get thread by coluname.");
            //return false;
        }
        if(isExit()){
            pErrorInfo.set(this.errorCodeInfo_);
            return false;
        }
        int threadIndex = threadhashkey % threads_.size();
        WriterThread writerThread=threads_.get(threadIndex);
        synchronized (writerThread.writeQueue_) {
            writerThread.writeQueue_.add(row);
            if(writerThread.writeQueue_.size()>=batchSize_)
                writerThread.writeQueue_.notify();
        }
        return true;
    }
    public boolean insert(ErrorCodeInfo pErrorInfo, Object... args){
        if(exit_){
            pErrorInfo.set(errorCodeInfo_);
            return false;
        }
        if(args.length!=colTypes_.size()){
            pErrorInfo.set(ErrorCodeInfo.Code.EC_InvalidParameter,"Parameter length mismatch "+
                    args.length+" expect "+colTypes_.size());
            return false;
        }
        try {
            List<Entity> prow=new ArrayList<>();
            int colindex = 0;
            Entity.DATA_TYPE dataType;
            for (Object one : args) {
                dataType = colTypes_.get(colindex);
                Scalar scalar;
                if (one == null) {
                    scalar = BasicEntityFactory.instance().createScalarWithDefaultValue(dataType);
                    scalar.setNull();
                } else {
                    scalar = BasicEntityFactory.createScalar(dataType, one);
                    if (scalar == null) {
                        pErrorInfo.set(ErrorCodeInfo.Code.EC_InvalidParameter, "Invalid object " + one + " for type " + dataType);
                        return false;
                    }
                }
                prow.add(scalar);
                colindex++;
            }
            int threadindex;
            if(threads_.size() > 1){
                if(isPartionedTable_){
                    Vector pvector=BasicEntityFactory.instance().createVectorWithDefaultValue(colTypes_.get(partitionColumnIdx_),1);
                    if(prow.get(partitionColumnIdx_) != null){
                        pvector.set(0, (Scalar) prow.get(partitionColumnIdx_));
                        List<Integer> indexes = partitionDomain_.getPartitionKeys(pvector);
                        if(indexes.isEmpty()==false){
                            threadindex = indexes.get(0);
                        }else{
                            pErrorInfo.set(ErrorCodeInfo.Code.EC_Server,"getPartitionKeys failed.");
                            return false;
                        }
                    }
                    else {
                        threadindex = 0;
                    }
                }else{
                    if (prow.get(threadByColIndexForNonPartion_) != null) {
                        Vector pvector=BasicEntityFactory.instance().createVectorWithDefaultValue(colTypes_.get(threadByColIndexForNonPartion_),1);
                        pvector.set(0,(Scalar) prow.get(threadByColIndexForNonPartion_));
                        threadindex =pvector.hashBucket(0,threads_.size());
                    }
                    else {
                        threadindex = 0;
                    }
                }
            }else{
                threadindex = 0;
            }
            return insertThreadWrite(threadindex, prow, pErrorInfo);
        }catch (Exception e){
            pErrorInfo.set(ErrorCodeInfo.Code.EC_InvalidParameter, "Invalid object error " + e);
            return false;
        }
    }
    private boolean isExit() { return exit_; }
    private DBConnection newConn() throws IOException {
        DBConnection pConn = new DBConnection(false,useSSL_, compress_);
        //String hostName, int port, String userId, String password, String initialScript, boolean highAvailability, String[] highAvailabilitySites
        boolean ret = pConn.connect(hostName_, port_, userId_, password_, initialScript_,highAvailability_,highAvailabilitySites_);
        if (!ret)
            return null;
        return pConn;
    }
    private void setExit(ErrorCodeInfo errorCodeInfo) {
        if (exit_)
            return;
        exit_ = true;
        errorCodeInfo_ = errorCodeInfo;
    }

}
