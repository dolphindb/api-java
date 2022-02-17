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
        long sentRows,unwriteRows,failedRows;
    };
    public static class Status{
        boolean isExit;
        ErrorCodeInfo errorInfo;
        long sentRows, unwriteRows, failedRows;
        List<ThreadStatus> threadStatusList=new ArrayList<>();
    };
    static class WriterThread implements Runnable{
        WriterThread(MultithreadTableWriter tableWriter,DBConnection conn) {
            tableWriter_=tableWriter;
            sentRows_=0;
            conn_ = conn;
            exit_ = false;
            writeThread_ = new Thread(this);
            writeThread_.start();
        }
        @Override
        public void run(){
            if (init() == false)//init
                return;
            long batchWaitTimeout, diff;
            while (isExit() == false) {
                try {
                    synchronized (writeQueue_) {
                        //tableWriter_.logger_.info(writeThread_.getId()+" run wait0 start");
                        writeQueue_.wait();
                        //tableWriter_.logger_.info(writeThread_.getId()+" run wait0 end");
                        if (isExit() ==false && tableWriter_.batchSize_ > 1 && tableWriter_.throttleMilsecond_ > 0) {
                            batchWaitTimeout = System.currentTimeMillis() + tableWriter_.throttleMilsecond_;
                            //tableWriter_.logger_.info(writeThread_.getId()+" run wait1 start");
                            while (isExit() ==false && writeQueue_.size() < tableWriter_.batchSize_) {//check batchsize
                                diff = batchWaitTimeout - System.currentTimeMillis();
                                if (diff > 0) {
                                    writeQueue_.wait(diff);
                                }
                                else {
                                    break;
                                }
                            }
                            //tableWriter_.logger_.info(writeThread_.getId()+" run wait1 end");
                        }
                    }
                    //tableWriter_.logger_.info(writeThread_.getId()+" run writeAllData start");
                    while(isExit() == false && writeAllData());
                    //tableWriter_.logger_.info(writeThread_.getId()+" run writeAllData end");
                } catch (Exception e) {
                    e.printStackTrace();
                    break;
                }
            }
            //tableWriter_.logger_.info(writeThread_.getId()+" run writeAllData2 start");
            while(tableWriter_.isExit()==false && writeAllData());
            //tableWriter_.logger_.info(writeThread_.getId()+" run writeAllData2 end");
            synchronized (writeThread_){
                writeThread_.notify();
            }
        }
        boolean writeAllData(){
            List<List<Entity>> items=new ArrayList<>();
            synchronized (writeQueue_) {
                items.addAll(writeQueue_);
                writeQueue_.clear();
            }
            int size = items.size();
            if (size < 1)
                return false;
            //std::cout << "writeTableAllData size:" << size << std::endl;
            //tableWriter_.logger_.info(" writeAllData=" + size);
            boolean isWriteDone = true;
            BasicTable writeTable = null;
            int addRowCount = 0;
            try {//create table
                //RECORDTIME("MTTW:createTable");
                //tableWriter_.logger_.info(" createTable=" + size);
                List<Vector> columns = new ArrayList<>();
                for (Entity.DATA_TYPE one : tableWriter_.colTypes_) {
                    columns.add(BasicEntityFactory.instance().createVectorWithDefaultValue(one, size));
                }
                writeTable = new BasicTable(tableWriter_.colNames_, columns);
                for (List<Entity> row : items) {
                    try {
                        for(int colindex = 0; colindex < columns.size(); colindex++){
                            Vector col = columns.get(colindex);
                            Scalar scalar = (Scalar) row.get(colindex);
                            if (scalar != null)
                                col.set(addRowCount, scalar);
                            else
                                col.setNull(addRowCount);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        isWriteDone = false;
                        tableWriter_.logger_.warning("threadid=" + writeThread_.getId() + " sendindex=" + sentRows_ + " Append row failed: " + e);
                        tableWriter_.setError(ErrorCodeInfo.Code.EC_InvalidObject, "Append row failed: " + e);
                        break;
                    }
                    addRowCount++;
                }
            } catch (Exception e) {
                e.printStackTrace();
                tableWriter_.logger_.warning("threadid=" + writeThread_.getId() + " Create table error: " + e);
                tableWriter_.setError(ErrorCodeInfo.Code.EC_InvalidObject, "Create table error: " + e);
                isWriteDone = false;
                //std::cerr << Util::createTimestamp(Util::getEpochTime())->getString() << " Backgroud thread of table (" << tableWriter_.dbName_ << " " << tableWriter_.tableName_ << "). Failed to send data to server, with exception: " << e.what() << std::endl;
            }
            if (isWriteDone && writeTable != null && addRowCount > 0) {//may contain empty vector for exit
                String runscript = "";
                try {//save table
                    //RECORDTIME("MTTW:saveTable");
                    //tableWriter_.logger_.info(" saveTable=" + size);
                    List<Entity> args = new ArrayList<>();
                    args.add(writeTable);
                    runscript = scriptTableInsert_;
                    conn_.run(runscript, args);
                    if (scriptSaveTable_ != null && scriptSaveTable_.isEmpty() == false) {
                        runscript = scriptSaveTable_;
                        conn_.run(runscript);
                    }
                    sentRows_ += addRowCount;
                } catch (Exception e) {
                    e.printStackTrace();
                    tableWriter_.logger_.warning("threadid=" + writeThread_.getId() + " sendindex=" + sentRows_ + " Save table error: " + e + " script:" + runscript);
                    tableWriter_.setError(ErrorCodeInfo.Code.EC_Server, "Save table error: " + e + " script: " + runscript);
                    conn_ = null;
                    isWriteDone = false;
                    //std::cerr << Util::createTimestamp(Util::getEpochTime())->getString() << " Backgroud thread of table (" << tableWriter_.dbName_ << " " << tableWriter_.tableName_ << "). Failed to send data to server, with exception: " << e.what() << std::endl;
                }
            }
            //tableWriter_.logger_.info(" done=" + isWriteDone);
            if (isWriteDone == false) {
                synchronized (failedQueue_) {
                    failedQueue_.addAll(items);
                }
            }
            return true;
        }
        boolean init(){
            if (tableWriter_.tableName_.isEmpty()) {
                scriptTableInsert_ = "tableInsert{\"" + tableWriter_.dbName_ + "\"}";
            }
            else if (tableWriter_.isPartionedTable_) {//partitioned table
                scriptTableInsert_ = "tableInsert{loadTable(\"" + tableWriter_.dbName_ + "\",\"" + tableWriter_.tableName_ + "\")}";
            }
            else {// single partitioned table
                scriptTableInsert_ = "tableInsert{loadTable(\"" + tableWriter_.dbName_ + "\",\"" + tableWriter_.tableName_ + "\")}";
                /*{
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
                        e.printStackTrace();
                        tableWriter_.logger_.warning("threadid=" + writeThread_.getId()+ " Init table error: "+e+ " script:"+scriptCreateTmpTable);
                        setError(ErrorCodeInfo.Code.EC_Server, "Init table error: " + e + " script: " + scriptCreateTmpTable);
                        conn_ = null;
                        //std::cerr << Util::createTimestamp(Util::getEpochTime())->getString() << " Backgroud thread of table (" << tableWriter_.dbName_ << " " << tableWriter_.tableName_ << "). Failed to init data to server, with exception: " << e.what() << std::endl;
                        return false;
                    }
                }
                scriptTableInsert_ = "tableInsert{tempTable}";
                scriptSaveTable_ = "saveTable(database(\"" + tableWriter_.dbName_ + "\")" + ",tempTable,\"" + tableWriter_.tableName_ + "\", 1);tempTable.clear!();";
                */
            }
            return true;
        }
        void getStatus(ThreadStatus status){
            status.threadId = writeThread_.getId();
            status.sentRows = sentRows_;
            status.unwriteRows = writeQueue_.size();
            status.failedRows = failedQueue_.size();
        }
        boolean isExit(){
            return exit_ || tableWriter_.hasError_;
        }
        void exit(){
            exit_ = true;
            synchronized (writeQueue_) {
                writeQueue_.notify();
            }
        }
        MultithreadTableWriter tableWriter_;
        DBConnection conn_;

        String scriptTableInsert_,scriptSaveTable_;

        List<List<Entity>> writeQueue_=new ArrayList<>();
        List<List<Entity>> failedQueue_=new ArrayList<>();
        Thread writeThread_;
        long sentRows_;
        boolean exit_;//Only set when exit
    };
    private String dbName_;
    private String tableName_;
    private int batchSize_;
    private int throttleMilsecond_;
    private boolean isPartionedTable_, hasError_;
    private String partitionedColName_;
    private List<String> colNames_=new ArrayList<>(),colTypeString_=new ArrayList<>();
    private List<Entity.DATA_TYPE> colTypes_=new ArrayList<>();
    private Domain partitionDomain_;
    private int partitionColumnIdx_;
    private int threadByColIndexForNonPartion_;
    private List<WriterThread> threads_=new ArrayList<>();
    private ErrorCodeInfo errorCodeInfo_;
    public MultithreadTableWriter(String hostName, int port, String userId, String password,
                                  String dbName, String tableName, boolean useSSL,
                                  boolean highAvailability, String[] highAvailabilitySites,
                                  int batchSize, float throttle,
                                  int threadCount, String partitionedColName) throws Exception{
        dbName_=dbName;
        tableName_=tableName;
        batchSize_=batchSize;
        throttleMilsecond_=(int)throttle*1000;
        hasError_=false;
        if(threadCount < 1){
            throw new RuntimeException("Thread count must be greater or equal than 1.");
        }
        if(batchSize < 1){
            throw new RuntimeException("Batch size must be greater than 1.");
        }
        if(throttle < 0){
            throw new RuntimeException("Throttle must be greater than 0.");
        }
        if (threadCount > 1 && partitionedColName.length()<1) {
            throw new RuntimeException("PartitionedColName must be specified in muti-thread mode.");
        }
        DBConnection pConn = newConn(hostName,port,userId,password,dbName,tableName,useSSL,highAvailability,highAvailabilitySites);
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
        if(partColNames!=null){//partitioned table
            isPartionedTable_ = true;
        }else{//没有分区
            if(tableName.isEmpty() == false){//Single partitioned table
                if(threadCount > 1){
                    throw new RuntimeException("Single partitioned table support single thread only.");
                }
            }
            isPartionedTable_ = false;
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
                if (partColNames.getString().equals(partitionedColName) == false) {
                    throw new RuntimeException("PartitionColumnName mismatch specified value, is "+ partColNames.getString()+" ?");
                }
                partitionColumnIdx_ = ((BasicInt)schema.get(new BasicString("partitionColumnIndex"))).getInt();
                partitionSchema = schema.get(new BasicString("partitionSchema"));
                partitionType =  ((BasicInt)schema.get(new BasicString("partitionType"))).getInt();
            }else{
                BasicStringVector partColNamesVec = (BasicStringVector)partColNames;
                int dims = partColNamesVec.rows();
                if(dims > 1 && partitionedColName.isEmpty()){
                    throw new RuntimeException("Please specify threadByColName for this partitioned table.");
                }
                int index = -1;
                for(int i=0; i<dims; ++i){
                    if(partColNamesVec.getString(i).equals(partitionedColName)){
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
            if(partitionedColName.isEmpty() == false){
                int threadcolindex = -1;
                for(int i=0; i<colNames_.size(); i++){
                    if(colNames_.get(i).equals(partitionedColName)){
                        threadcolindex=i;
                        break;
                    }
                }
                if(threadcolindex < 0){
                    throw new RuntimeException("Can't find column name for "+partitionedColName);
                }
                threadByColIndexForNonPartion_=threadcolindex;
            }
        }
        // init done, start thread now.
        for(int i = 0; i < threadCount; i++){
            if (pConn == null) {
                pConn = newConn(hostName,port,userId,password,dbName,tableName,useSSL,highAvailability,highAvailabilitySites);
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
                writeThread.failedQueue_.clear();
            }
            synchronized (writeThread.writeQueue_){
                unwrite.addAll(writeThread.writeQueue_);
                writeThread.writeQueue_.clear();
            }
        }
    }
    public void getStatus(Status status){
        status.errorInfo=errorCodeInfo_;
        status.failedRows=status.sentRows=status.unwriteRows=0;
        status.isExit=isExit();
        for(WriterThread writeThread : threads_){
            ThreadStatus threadStatus=new ThreadStatus();
            writeThread.getStatus(threadStatus);
            status.threadStatusList.add(threadStatus);

            status.sentRows += threadStatus.sentRows;
            status.unwriteRows += threadStatus.unwriteRows;
            status.failedRows += threadStatus.failedRows;
        }
    }
    public void waitExit() throws InterruptedException{
        for(WriterThread one:threads_){
            one.exit();
        }
        for (WriterThread one : threads_) {
            if (one.writeThread_.isAlive()) {
                synchronized (one.writeThread_){
                    one.writeThread_.wait(100);
                }
            }
            one.conn_=null;
        }
        threads_.clear();
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
                    e.printStackTrace();
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
                    e.printStackTrace();
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
            writerThread.writeQueue_.notify();
        }
        return true;
    }
    public boolean insert(ErrorCodeInfo pErrorInfo, Object... args){
        if(isExit()){
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
            e.printStackTrace();
            pErrorInfo.set(ErrorCodeInfo.Code.EC_InvalidParameter, "Invalid object error " + e);
            return false;
        }
    }
    private boolean isExit() { return hasError_; }
    private DBConnection newConn(String hostName, int port, String userId, String password,
                                 String dbName, String tableName, boolean useSSL,
                                 boolean highAvailability, String[] highAvailabilitySites) throws IOException {
        DBConnection pConn = new DBConnection(false,useSSL);
        //String hostName, int port, String userId, String password, String initialScript, boolean highAvailability, String[] highAvailabilitySites
        boolean ret = pConn.connect(hostName, port, userId, password, null,highAvailability,highAvailabilitySites);
        if (!ret)
            return null;
        return pConn;
    }
    private void setError(ErrorCodeInfo.Code code,String info) {
        if (hasError_)
            return;
        hasError_ = true;
        errorCodeInfo_ = new ErrorCodeInfo(code, info);
    }

}
