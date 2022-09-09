package com.xxdb.multithreadedtablewriter;

import com.xxdb.DBConnection;
import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.data.*;
import com.xxdb.route.Domain;
import com.xxdb.route.DomainFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class MultithreadedTableWriter {
    private Logger logger_=Logger.getLogger(getClass().getName());
    public static class ThreadStatus{
        public long threadId;
        public long sentRows,unsentRows,sendFailedRows;
        public String toString(){
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%16s", threadId) + String.format("%16s", sentRows) + String.format("%16s", unsentRows) + String.format("%16s", unsentRows) + "\n");
            return sb.toString();
        }
    };
    public static class Status extends ErrorCodeInfo{
        public boolean isExiting;
        public long sentRows, unsentRows, sendFailedRows;
        public List<ThreadStatus> threadStatusList=new ArrayList<>();

        public String toString(){
            StringBuilder sb = new StringBuilder();
            sb.append("errorCode     : " + errorCode + "\n");
            sb.append("errorInfo     : " + errorInfo + "\n");
            sb.append("isExiting     : " + isExiting + "\n");
            sb.append("sentRows      : " + sentRows + "\n");
            sb.append("unsentRows    : " + unsentRows + "\n");
            sb.append("sendFailedRows: " + sendFailedRows + "\n");
            sb.append("threadStatus  :" + "\n");
            sb.append(String.format("%16s", "threadId") + String.format("%16s", "sentRows")
                    + String.format("%16s", "unsentRows") + String.format("%16s", "sendFailedRows") + "\n");
            for (int i = 0; i < threadStatusList.size(); i++){
                sb.append(threadStatusList.get(i).toString());
            }
            return sb.toString();
        }
    };
    static class WriterThread implements Runnable{
        WriterThread(MultithreadedTableWriter tableWriter, DBConnection conn) {
            tableWriter_=tableWriter;
            sentRows_=0;
            conn_ = conn;
            exit_ = false;
            isFinished_ = false;
            writeQueue_.add(tableWriter_.createListVector());
            writeThread_ = new Thread(this);
            writeThread_.start();
        }
        @Override
        public void run(){
            if (!init())//init
                return;
            long batchWaitTimeout, diff;
            try {
                while (!isExiting()) {
                    synchronized (writeQueue_) {
                        //tableWriter_.logger_.info(writeThread_.getId()+" run wait0 start");
                        //if (!isExiting()&&writeQueue_.size()==0)
                        writeQueue_.wait();
                        if (!isExiting() && tableWriter_.batchSize_ > 1 && tableWriter_.throttleMilsecond_ > 0) {
                            batchWaitTimeout = System.currentTimeMillis() + tableWriter_.throttleMilsecond_;
                            while (!isExiting() && (writeQueue_.size() - 1) * vectorSize + writeQueue_.get(writeQueue_.size() - 1).get(0).rows() < tableWriter_.batchSize_) {//check batchsize
                                diff = batchWaitTimeout - System.currentTimeMillis();
                                if (diff > 0) {
                                    writeQueue_.wait(diff);
                                } else {
                                    break;
                                }
                            }
                        }
                    }
                    while (!isExiting() && writeAllData());
                }
                while (!tableWriter_.hasError_ && writeAllData()) ;
            }catch (Exception e) {
                    e.printStackTrace();
                    tableWriter_.hasError_ = true;
                    tableWriter_.errorCodeInfo_.set(ErrorCodeInfo.Code.EC_None, e.getMessage());
            }
            synchronized (writeThread_){
                conn_.close();
                isFinished_ = true;
                writeThread_.notify();
            }
        }

        boolean writeAllData(){
            synchronized (busyLock_) {
                List<Vector> items = new ArrayList<>();
                int addRowCount = 0;
                synchronized (writeQueue_) {
                    items = writeQueue_.get(0);
                    addRowCount = items.get(0).rows();
                    if (addRowCount < 1)
                        return false;
                    writeQueue_.remove(0);
                    if (writeQueue_.size() == 0)
                        writeQueue_.add(tableWriter_.createListVector());
                }

                boolean isWriteDone = true;
                BasicTable writeTable = null;
                try {
                    writeTable = new BasicTable(tableWriter_.colNames_, items);
                }catch (Exception e){
                    e.printStackTrace();
                    tableWriter_.logger_.warning("threadid=" + writeThread_.getId() + " sendindex=" + sentRows_ + " create table error: " + e);
                    tableWriter_.setError(ErrorCodeInfo.Code.EC_Server, "Failed to createTable: " + e);
                    isWriteDone = false;
                }
                if (isWriteDone) {//may contain empty vector for exit
                    String runscript = "";
                    try {//save table
                        List<Entity> args = new ArrayList<>();
                        args.add(writeTable);
                        runscript = scriptTableInsert_;
                        conn_.run(runscript, args);
                        if (scriptSaveTable_ != null && !scriptSaveTable_.isEmpty()) {
                            runscript = scriptSaveTable_;
                            conn_.run(runscript);
                        }
                        sentRows_ += addRowCount;
                    } catch (Exception e) {
                        e.printStackTrace();
                        tableWriter_.logger_.warning("threadid=" + writeThread_.getId() + " sendindex=" + sentRows_ + " Save table error: " + e + " script:" + runscript);
                        tableWriter_.setError(ErrorCodeInfo.Code.EC_Server, "Failed to save the inserted data: " + e + " script: " + runscript);
                        isWriteDone = false;
                        tableWriter_.hasError_ = true;
                    }
                }
                if (!isWriteDone) {
                    synchronized (failedQueue_) {
                        int cols = items.size();
                        int rows = items.get(0).rows();
                        for (int i = 0; i < rows; i++){
                            List<Entity> tmp = new ArrayList<>();
                            for (int j = 0; j < cols; j++){
                                if (tableWriter_.colTypes_.get(j).getValue() < 65)
                                    tmp.add(items.get(j).get(i));
                                else
                                    tmp.add(((BasicArrayVector) items.get(j)).getVectorValue(i));
                            }
                            failedQueue_.add(tmp);
                        }
                    }
                }
                boolean startgc = false;
                synchronized (tableWriter_) {
                    tableWriter_.sentRowsAfterGc_ += addRowCount;
                    if (tableWriter_.sentRowsAfterGc_ > 10000) {//every sent 10000 rows, manual start gc to avoid out of memory
                        tableWriter_.sentRowsAfterGc_ = 0;
                        startgc = true;
                    }
                }
                if (startgc && Runtime.getRuntime().freeMemory() < 104857600)
                    System.gc();
            }
            return true;
        }

        boolean init(){
            if (tableWriter_.dbName_.isEmpty()) {
                scriptTableInsert_ = "tableInsert{\"" + tableWriter_.tableName_ + "\"}";
            }
            else if (tableWriter_.isPartionedTable_) {//partitioned table
                scriptTableInsert_ = "tableInsert{loadTable(\"" + tableWriter_.dbName_ + "\",\"" + tableWriter_.tableName_ + "\")}";
            }
            else {// single partitioned table
                scriptTableInsert_ = "tableInsert{loadTable(\"" + tableWriter_.dbName_ + "\",\"" + tableWriter_.tableName_ + "\")}";
            }
            return true;
        }

        void getStatus(ThreadStatus status){
            status.threadId = writeThread_.getId();
            status.sentRows = sentRows_;
            synchronized (writeQueue_){
                status.unsentRows = (long) (writeQueue_.size() - 1) *  vectorSize+ writeQueue_.get(writeQueue_.size() - 1).get(0).rows();
            }
            status.sendFailedRows = failedQueue_.size();
        }

        boolean isExiting(){
            return exit_ || tableWriter_.hasError_;
        }
        void exit(){
            synchronized (writeQueue_) {
                exit_ = true;
                writeQueue_.notify();
            }
        }
        MultithreadedTableWriter tableWriter_;
        DBConnection conn_;
        Object busyLock_ = new Object();

        String scriptTableInsert_,scriptSaveTable_;

        List<List<Vector>> writeQueue_ = new ArrayList<>();
        List<List<Entity>> failedQueue_=new ArrayList<>();
        Thread writeThread_;
        long sentRows_;
        boolean exit_;//Only set when exit
        boolean isFinished_;
        public static int vectorSize = 65536;
    };
    private String dbName_;
    private String tableName_;
    private int batchSize_;
    private int throttleMilsecond_;
    private boolean isPartionedTable_;
    private boolean hasError_;
    private boolean isExiting_ = false;
    private String partitionedColName_;
    private List<String> colNames_=new ArrayList<>(),colTypeString_=new ArrayList<>();
    private List<Entity.DATA_TYPE> colTypes_=new ArrayList<>();
    private int[] compressTypes_=null;
    private Domain partitionDomain_;
    private int partitionColumnIdx_;
    private int threadByColIndexForNonPartion_;
    private int sentRowsAfterGc_;//manual start gc after sent some rows
    private List<WriterThread> threads_=new ArrayList<>();
    private ErrorCodeInfo errorCodeInfo_ = new ErrorCodeInfo();
    public MultithreadedTableWriter(String hostName, int port, String userId, String password,
                                    String dbName, String tableName, boolean useSSL,
                                    boolean enableHighAvailability, String[] highAvailabilitySites,
                                    int batchSize, float throttle,
                                    int threadCount, String partitionCol,
                                    int[] compressTypes) throws Exception{
        init(hostName,port,userId, password,dbName, tableName, useSSL,enableHighAvailability,highAvailabilitySites,
                batchSize, throttle,threadCount,partitionCol,compressTypes);
    }
    public MultithreadedTableWriter(String hostName, int port, String userId, String password,
                                    String dbName, String tableName, boolean useSSL,
                                    boolean enableHighAvailability, String[] highAvailabilitySites,
                                    int batchSize, float throttle,
                                    int threadCount, String partitionCol) throws Exception{
        init(hostName,port,userId, password,dbName, tableName, useSSL,enableHighAvailability,highAvailabilitySites,
                batchSize, throttle,threadCount,partitionCol,null);
    }
    private void init(String hostName, int port, String userId, String password,
                      String dbName, String tableName, boolean useSSL,
                      boolean enableHighAvailability, String[] highAvailabilitySites,
                      int batchSize, float throttle,
                      int threadCount, String partitionCol,
                      int[] compressTypes) throws Exception{
        dbName_=dbName;
        tableName_=tableName;
        batchSize_=batchSize;
        throttleMilsecond_=(int)throttle*1000;
        hasError_=false;
        if(threadCount < 1){
            throw new RuntimeException("The parameter threadCount must be greater than or equal to 1.");
        }
        if(batchSize < 1){
            throw new RuntimeException("The parameter batchSize must be greater than or equal to 1.");
        }
        if(throttle < 0){
            throw new RuntimeException("The parameter throttle must be greater than or equal to 0.");
        }
        if (threadCount > 1 && partitionCol.length()<1) {
            throw new RuntimeException("The parameter partitionCol must be specified when threadCount is greater than 1.");
        }
        boolean isCompress = false;
        if (compressTypes != null && compressTypes.length > 0) {
            for (int one : compressTypes) {
                if (one != Vector.COMPRESS_LZ4 && one != Vector.COMPRESS_DELTA) {
                    throw new RuntimeException("Unsupported compress method " + one);
                }
            }
            isCompress = true;
            compressTypes_=new int[compressTypes.length];
            System.arraycopy(compressTypes,0,compressTypes_,0,compressTypes.length);
        }
        DBConnection pConn = newConn(hostName,port,userId,password,dbName,tableName,useSSL,enableHighAvailability,highAvailabilitySites,isCompress);
        if(pConn==null){
            throw new RuntimeException("Failed to connect to server " + hostName + ":" + port);
        }

        BasicDictionary schema;
        if(dbName.isEmpty()){
            schema = (BasicDictionary)pConn.run("schema(" + tableName + ")");
        }else{
            schema = (BasicDictionary)pConn.run("schema(loadTable(\"" + dbName + "\",\"" + tableName + "\"))");
        }
        Entity partColNames = schema.get(new BasicString("partitionColumnName"));
        if(partColNames!=null){//partitioned table
            isPartionedTable_ = true;
        }else{//没有分区
            if(!dbName.isEmpty()){//Single partitioned table
                if(threadCount > 1){
                    throw new RuntimeException("The parameter threadCount must be 1 for a dimension table.");
                }
            }
            isPartionedTable_ = false;
        }

        BasicTable colDefs = (BasicTable)schema.get(new BasicString("colDefs"));
        BasicIntVector colDefsTypeInt = (BasicIntVector)colDefs.getColumn("typeInt");
        int columnSize = colDefs.rows();
        if (compressTypes_!=null && compressTypes_.length != columnSize) {
            throw new RuntimeException("The number of elements in parameter compressMethods does not match the column size "+columnSize);
        }

        BasicStringVector colDefsName = (BasicStringVector)colDefs.getColumn("name");
        BasicStringVector colDefsTypeString = (BasicStringVector)colDefs.getColumn("typeString");
        for(int i = 0; i < columnSize; i++){
            colNames_.add(colDefsName.getString(i));
            if (compressTypes_ != null){
                boolean check = AbstractVector.checkCompressedMethod(Entity.DATA_TYPE.valueOf(colDefsTypeInt.getInt(i)), compressTypes_[i]);
                if (check)
                    colTypes_.add(Entity.DATA_TYPE.valueOf(colDefsTypeInt.getInt(i)));
                else
                    throw new RuntimeException("Compression Failed: only support integral and temporal data, not support " + Entity.DATA_TYPE.valueOf(colDefsTypeInt.getInt(i)));
            }
            else{
                colTypes_.add(Entity.DATA_TYPE.valueOf(colDefsTypeInt.getInt(i)));
            }
            colTypeString_.add(colDefsTypeString.getString(i));
        }
        if(isPartionedTable_){
            Entity partitionSchema;
            int partitionType;
            if(partColNames.isScalar()){
                if (!partColNames.getString().equals(partitionCol)) {
                    throw new RuntimeException("The parameter partionCol must be the partitioning column "+ partColNames.getString()+" in the table.");
                }
                partitionColumnIdx_ = ((BasicInt)schema.get(new BasicString("partitionColumnIndex"))).getInt();
                partitionSchema = schema.get(new BasicString("partitionSchema"));
                partitionType =  ((BasicInt)schema.get(new BasicString("partitionType"))).getInt();
            }else{
                BasicStringVector partColNamesVec = (BasicStringVector)partColNames;
                int dims = partColNamesVec.rows();
                if(dims > 1 && partitionCol.isEmpty()){
                    throw new RuntimeException("The parameter partitionCol must be specified for a partitioned table.");
                }
                int index = -1;
                for(int i=0; i<dims; ++i){
                    if(partColNamesVec.getString(i).equals(partitionCol)){
                        index = i;
                        break;
                    }
                }
                if(index < 0)
                    throw new RuntimeException("The parameter partionCol must be the partitioning columns in the partitioned table. ");
                partitionColumnIdx_ = ((BasicIntVector)schema.get(new BasicString("partitionColumnIndex"))).getInt(index);
                partitionSchema = ((BasicAnyVector)schema.get(new BasicString("partitionSchema"))).getEntity(index);
                partitionType =  ((BasicIntVector)schema.get(new BasicString("partitionType"))).getInt(index);
            }
            Entity.DATA_TYPE dataColType = colTypes_.get(partitionColumnIdx_);
            Entity.PARTITION_TYPE partitionColtype=Entity.PARTITION_TYPE.values()[partitionType];
            partitionDomain_ = DomainFactory.createDomain(partitionColtype, dataColType, partitionSchema);
        } else {//isPartionedTable_==false
            if(partitionCol.isEmpty() == false){
                int threadcolindex = -1;
                for(int i=0; i<colNames_.size(); i++){
                    if(colNames_.get(i).equals(partitionCol)){
                        threadcolindex=i;
                        break;
                    }
                }
                if(threadcolindex < 0){
                    throw new RuntimeException("No match found for "+partitionCol);
                }
                threadByColIndexForNonPartion_=threadcolindex;
            }
        }
        // init done, start thread now.
        for(int i = 0; i < threadCount; i++){
            if (pConn == null) {
                pConn = newConn(hostName,port,userId,password,dbName,tableName,useSSL,enableHighAvailability,highAvailabilitySites,isCompress);
            }
            WriterThread writerThread = new WriterThread(this,pConn);
            threads_.add(writerThread);
            pConn = null;
        }
    }

    public List<List<Entity>> getUnwrittenData() {
        List<List<Entity>> unwrittenData = new ArrayList<>();
        for(WriterThread writeThread : threads_){
            synchronized (writeThread.busyLock_) {
                synchronized (writeThread.failedQueue_) {
                    unwrittenData.addAll(writeThread.failedQueue_);
                    writeThread.failedQueue_.clear();
                }
                synchronized (writeThread.writeQueue_) {
                    int cols = colTypes_.size();
                    int size = writeThread.writeQueue_.size();
                    for (int i = 0; i < size; ++i)
                    {
                        int rows = writeThread.writeQueue_.get(i).get(0).rows();
                        for (int row = 0; row < rows; ++row)
                        {
                            List<Entity> tmp = new ArrayList<>();
                            for (int j = 0; j < cols; ++j)
                            {
                                tmp.add(writeThread.writeQueue_.get(i).get(j).get(row));
                            }
                            unwrittenData.add(tmp);
                        }
                    }
                    unwrittenData.addAll(writeThread.failedQueue_);
                    writeThread.writeQueue_.clear();
                    writeThread.writeQueue_.add(createListVector());
                }
            }
        }
        return unwrittenData;
    }

    public List<List<Entity>> getFailedData() throws InterruptedException{
        List<List<Entity>> failedData = new ArrayList<>();
        for (WriterThread writeThread : threads_){
            synchronized (writeThread.busyLock_){
                synchronized (writeThread.failedQueue_){
                    failedData.addAll(writeThread.failedQueue_);
                    writeThread.failedQueue_.clear();
                }
            }
        }
        return  failedData;
    }

    public Status getStatus(){
        Status status = new Status();
        status.errorCode = errorCodeInfo_.errorCode;
        status.errorInfo = errorCodeInfo_.errorInfo;
        status.sendFailedRows=status.sentRows=status.unsentRows=0;
        status.isExiting=isExiting();
        for(WriterThread writeThread : threads_){
            ThreadStatus threadStatus=new ThreadStatus();
            writeThread.getStatus(threadStatus);
            status.threadStatusList.add(threadStatus);
            status.sentRows += threadStatus.sentRows;
            status.unsentRows += threadStatus.unsentRows;
            status.sendFailedRows += threadStatus.sendFailedRows;
        }
        return status;
    }

    public void waitForThreadCompletion() throws InterruptedException{
        isExiting_ = true;
        for(WriterThread one:threads_){
            one.exit();
        }
        for (WriterThread one : threads_) {
            synchronized (one.writeThread_) {
                if(!one.isFinished_) {
                    one.writeThread_.wait();
                }
            }
            one.conn_ = null;
        }
        setError(ErrorCodeInfo.Code.EC_None,"");
    }
    public ErrorCodeInfo insertUnwrittenData(List<List<Entity>> records){
        if(isExiting()){
            throw new RuntimeException("Thread is exiting. ");
        }
        if(threads_.size() > 1){
            if(isPartionedTable_){
                Vector pvector=BasicEntityFactory.instance().createVectorWithDefaultValue(colTypes_.get(partitionColumnIdx_),records.size());
                int rowindex=0;
                try {
                    for (List<Entity> row : records) {
                        if (row.size() != colTypes_.size()) {
                            return new ErrorCodeInfo(ErrorCodeInfo.Code.EC_InvalidParameter, "Column counts don't match.");
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
                    return new ErrorCodeInfo(ErrorCodeInfo.Code.EC_InvalidParameter, "Row in records " + rowindex +
                            " mismatch type " + colTypes_.get(partitionColumnIdx_));
                }
                List<Integer> threadindexes = partitionDomain_.getPartitionKeys(pvector);
                try {
                    for(int row = 0; row < threadindexes.size(); row++){
                        insertThreadWrite(threadindexes.get(row), records.get(row));
                    }
                }catch (Exception e){
                }
            }else{
                Vector partionvector=BasicEntityFactory.instance().createVectorWithDefaultValue(colTypes_.get(threadByColIndexForNonPartion_),records.size());
                int rowindex=0;
                try{
                    for(List<Entity> row : records) {
                        if (row.size() != colTypes_.size()) {
                            return new ErrorCodeInfo(ErrorCodeInfo.Code.EC_InvalidParameter, "Column counts don't match.");
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
                    return new ErrorCodeInfo(ErrorCodeInfo.Code.EC_InvalidParameter, "Row in records " + rowindex +
                            " mismatch type " + colTypes_.get(partitionColumnIdx_));
                }
                int threadindex;
                try {
                    for(rowindex=0;rowindex<records.size();rowindex++){
                        threadindex=partionvector.hashBucket(rowindex,threads_.size());
                        insertThreadWrite(threadindex, records.get(rowindex));
                    }
                }catch (Exception e){
                }
            }
        }else{
            try {
                for(List<Entity> row : records){
                    insertThreadWrite(0, row);
                }
            }catch (Exception e){
            }
        }
        return new ErrorCodeInfo();
    }

    private void insertThreadWrite(int threadhashkey, List<Entity> row) throws Exception{
        if(threadhashkey < 0)
            threadhashkey = 0;
        int threadIndex = threadhashkey % threads_.size();
        WriterThread writerThread=threads_.get(threadIndex);
        synchronized (writerThread.writeQueue_) {
            int rows = writerThread.writeQueue_.get(writerThread.writeQueue_.size() - 1).get(0).rows();
            if (rows > WriterThread.vectorSize){
                writerThread.writeQueue_.add(createListVector());
            }

            int size = row.size();
            for (int i = 0; i < size; i++){
                if (colTypes_.get(i).getValue() < 65){
                    writerThread.writeQueue_.get(writerThread.writeQueue_.size()-1).get(i).Append((Scalar) row.get(i));
                }else {
                    writerThread.writeQueue_.get(writerThread.writeQueue_.size()-1).get(i).Append((Vector) row.get(i));
                }
            }
            if (writerThread.writeQueue_.get(writerThread.writeQueue_.size()-1).get(0).rows() >= batchSize_)
                writerThread.writeQueue_.notify();
        }
    }

    public ErrorCodeInfo insert(Object... args){
        if(isExiting()){
            throw new RuntimeException("Thread is exiting. ");
        }
        if(args.length!=colTypes_.size()){
            return new ErrorCodeInfo(ErrorCodeInfo.Code.EC_InvalidParameter,"Column counts don't match.");
        }
        try {
            List<Entity> prow=new ArrayList<>();
            int colindex = 0;
            Entity.DATA_TYPE dataType;
            boolean isAllNull = true;
            for (Object one : args) {
                dataType = colTypes_.get(colindex);
                Entity entity;
                isAllNull = false;
                entity = BasicEntityFactory.createScalar(dataType, one);
                if (entity == null) {
                    return new ErrorCodeInfo(ErrorCodeInfo.Code.EC_InvalidObject, "Data conversion error: " + dataType);
                }
                prow.add(entity);
                colindex++;
            }
            if(isAllNull){
                return new ErrorCodeInfo(ErrorCodeInfo.Code.EC_InvalidObject, "Can't insert a Null row.");
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
                            return new ErrorCodeInfo(ErrorCodeInfo.Code.EC_Server,"Failed to obtain the partition scheme.");
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
            insertThreadWrite(threadindex, prow);
            return new ErrorCodeInfo();
        }catch (Exception e){
            e.printStackTrace();
            return new ErrorCodeInfo(ErrorCodeInfo.Code.EC_InvalidObject, "Invalid object error " + e);
        }
    }

    private List<Vector> createListVector(){
        int cols = colTypes_.size();
        List<Vector> tmp = new ArrayList<>();
        for (int i = 0; i < cols; i++){
            Entity.DATA_TYPE type = colTypes_.get(i);
            if (type.getValue() >= 65)
                tmp.add(new BasicArrayVector(type, 1));
            else
                tmp.add(BasicEntityFactory.instance().createVectorWithDefaultValue(type, 0));
        }
        return tmp;
    }

    private boolean isExiting() { return hasError_ || isExiting_; }
    private DBConnection newConn(String hostName, int port, String userId, String password,
                                 String dbName, String tableName, boolean useSSL,
                                 boolean enableHighAvailability, String[] highAvailabilitySites,boolean compress) throws IOException {
        DBConnection pConn = new DBConnection(false,useSSL,compress);
        //String hostName, int port, String userId, String password, String initialScript, boolean enableHighAvailability, String[] highAvailabilitySites
        boolean ret = pConn.connect(hostName, port, userId, password, null,enableHighAvailability,highAvailabilitySites);
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
