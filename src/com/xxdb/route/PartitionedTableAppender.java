package com.xxdb.route;

import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.data.Vector;

import java.io.IOException;
import java.util.*;
import java.util.Set;
import java.util.concurrent.*;

/**
 * PartitionedTableAppender is used to append rows to a partitioned table
 * across a cluster of DolphinDB instances.
 *
 * <pre>
 * {@code
 *
 * PartitionedTableAppender tableAppender = new PartitionedTableAppender("Trades", "192.168.1.25", 8848);
 * List<Entity> row = new ArrayList<>();
 * row.add(BasicInt(1));
 * row.add(BasicString('A'));
 * int affected = tableAppender.append(row);
 *
 * // append multiple rows at a time
 * List<Entity> rows = new ArrayList<>();
 * BasicIntVector vint = new BasicIntVector(Arrays.asList(1,2,3,4,5));
 * BasicStringVector vstring = new BasicStringVector(Arrays.asList("A", "B", "C", "D", "E"));
 * rows.add(vint);
 * rows.add(vstring);
 * affected = tableAppender.append(rows);
 *
 *
 * // cleanup
 * tableAppender.shutdownThreadPool();
 * }
 * </pre>
 */
public class PartitionedTableAppender {
    private static final int CORES = Runtime.getRuntime().availableProcessors();
    private Map<String, DBConnection> connectionMap = new HashMap<>();
    private BasicDictionary tableInfo;
    private TableRouter router;
    private BasicString tableName;
    private int partitionColumnIdx;
    private int cols;
    private Entity.DATA_CATEGORY columnCategories[];
    private Entity.DATA_TYPE columnTypes[];
    private int threadCount;
    private ExecutorService threadPool;

    /**
     *
     * @param tableName name of the shared table
     * @param host host
     * @param port port
     */
    public PartitionedTableAppender(String tableName, String host, int port) throws IOException {
        this(tableName, host, port, 0);
    }

    public PartitionedTableAppender(String tableName, String host, int port, int threadCount) throws IOException {
        this.tableName = new BasicString(tableName);
        DBConnection conn = new DBConnection();
        BasicAnyVector locations;
        AbstractVector partitionSchema;
        BasicTable colDefs;
        BasicIntVector typeInts;
        try {
            conn.connect(host, port);
            tableInfo = (BasicDictionary) conn.run("schema(" + tableName+ ")");
            partitionColumnIdx = ((BasicInt) tableInfo.get(new BasicString("partitionColumnIndex"))).getInt();
            if (partitionColumnIdx == -1) {
                throw new RuntimeException("Table '" + tableName + "' is not partitioned");
            }
            locations = (BasicAnyVector) tableInfo.get(new BasicString("partitionSites"));
            int partitionType = ((BasicInt) tableInfo.get(new BasicString("partitionType"))).getInt();
            partitionSchema = (AbstractVector) tableInfo.get(new BasicString("partitionSchema"));
            router = TableRouterFacotry.createRouter(Entity.PARTITION_TYPE.values()[partitionType], partitionSchema, locations);
            colDefs = ((BasicTable) tableInfo.get(new BasicString("colDefs")));
            this.cols = colDefs.getColumn(0).rows();
            typeInts = (BasicIntVector) colDefs.getColumn("typeInt");
            this.columnCategories = new Entity.DATA_CATEGORY[this.cols];
            this.columnTypes = new Entity.DATA_TYPE[this.cols];
            for (int i = 0; i < cols; ++i) {
                this.columnTypes[i] = Entity.DATA_TYPE.values()[typeInts.getInt(i)];
                this.columnCategories[i] = Entity.typeToCategory(this.columnTypes[i]);
            }
        } catch (IOException e) {
            throw e;
        } finally {
            conn.close();
        }

        this.threadCount = threadCount;
        if (this.threadCount <= 0) {
            this.threadCount = Math.min(CORES, locations.rows());
        }
        if (this.threadCount > 0) {
            this.threadCount--;
        }
        threadPool = Executors.newFixedThreadPool(this.threadCount);
    }

    private String getDestination(Scalar partitioningColumn) {
        return router.route(partitioningColumn);
    }
    private DBConnection getConnection(Entity partitioningColumn) throws IOException{
        if (!(partitioningColumn instanceof  Scalar))
            throw new RuntimeException("partitioning column value must be a scalar");
        String dest = getDestination((Scalar) partitioningColumn);
        DBConnection conn = connectionMap.get(dest);
        if (conn == null) {
            conn = new DBConnection();
            String[] destParts = dest.split(":");
            conn.connect(destParts[0], Integer.valueOf(destParts[1]));
            connectionMap.put(dest, conn);
        }

        return conn;
    }
    private static final BasicEntityFactory entityFactory = new BasicEntityFactory();
    private static final int CHECK_RESULT_SINGLE_ROW = 1;
    private static final int CHECK_RESULT_MULTI_ROWS = 2;
    private class BatchAppendTask implements Callable<Integer>{
        List<List<Scalar>> columns;
        DBConnection conn;
        BatchAppendTask(int cols, DBConnection conn) {
            this.columns = new ArrayList<>(cols);
            this.conn = conn;
        }
        public Integer call() {
            List<Entity> args = new ArrayList<>(1 + columns.size());
            args.add(tableName);
            for (int i = 0; i < columns.size(); ++i) {
                List<Scalar> column = columns.get(i);
                Vector vector = entityFactory.createVectorWithDefaultValue(column.get(0).getDataType(), column.size());
                for (int j = 0; j < column.size(); ++j) {
                    try {
                        vector.set(j, column.get(j));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                args.add(vector);
            }
            try {
                return ((BasicInt)conn.run("tableInsert", args)).getInt();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void appendColumn() {
            this.columns.add(new ArrayList<>());
        }
        public void appendToLastColumn(Scalar val) {
            columns.get(columns.size() - 1).add(val);
        }
    }
    // assume input is sanity checked.
    private int appendBatch(List<Entity> subTable) throws IOException {
        if (subTable.get(0).rows() == 0) {
            return 0;
        }
        List<DBConnection> destConns = new ArrayList<>();
        Map<DBConnection, BatchAppendTask> conn2TaskMap = new HashMap<>();
        AbstractVector partitioningColumnVector = (AbstractVector) subTable.get(partitionColumnIdx);
        int rows = partitioningColumnVector.rows();
        for (int i = 0; i < rows; ++i) {
            DBConnection conn = getConnection(partitioningColumnVector.get(i));
            if (!conn2TaskMap.containsKey(conn)) {
                conn2TaskMap.put(conn, new BatchAppendTask(this.cols, conn));
            }
            destConns.add(conn);
        }
        for (int i = 0; i < this.cols; ++i) {
            // for each task, add a new last column.
            Set<DBConnection> keySet = conn2TaskMap.keySet();
            for (DBConnection conn : keySet) {
                BatchAppendTask task = conn2TaskMap.get(conn);
                task.appendColumn();
            }
            // dispatch column to corresponding column
            AbstractVector column = (AbstractVector)subTable.get(i);
            for (int j = 0; j < rows; ++j) {
                DBConnection destConn = destConns.get(j);
                BatchAppendTask destTask = conn2TaskMap.get(destConn);
                destTask.appendToLastColumn(column.get(j));
            }
        }

        int affected = 0;
        BatchAppendTask savedTask = null;
        Set<DBConnection> keySet = conn2TaskMap.keySet();
        List<Future<Integer>> futures = new ArrayList<>();
        for (DBConnection conn : keySet) {
            BatchAppendTask task = conn2TaskMap.get(conn);
            if (savedTask == null) {
                savedTask = task;
            } else {
                futures.add(threadPool.submit(task));
            }
        }
        affected += savedTask.call();
        for (int i = 0; i < futures.size(); ++i) {
            try {
                affected += futures.get(i).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        return affected;
    }

    // assume input is sanity checked.
    private int appendSingle(List<Entity> row) throws IOException {
        DBConnection conn = getConnection(row.get(partitionColumnIdx));
        List<Entity> args = new ArrayList<>();
        args.add(tableName);
        args.addAll(row);
        return ((BasicInt)conn.run("tableInsert", args)).getInt();
    }
    private void checkColumnType(int col, Entity.DATA_CATEGORY category, Entity.DATA_TYPE type) {
        Entity.DATA_CATEGORY expectCategory = this.columnCategories[col];
        Entity.DATA_TYPE expectType = this.columnTypes[col];
        if (category != expectCategory) {
            throw new RuntimeException("column " + col + ", expect category " + expectCategory.name() + ", got category " + category.name());
        } else if (category == Entity.DATA_CATEGORY.TEMPORAL && type != expectType) {
            throw new RuntimeException("column " + col + ", temporal column must have exactly the same type, expect " + expectType.name() + ", got " + type.name() );
        }
    }
    private int check(List<Entity> row) {
        if (row.size() != cols) {
            throw new RuntimeException("expect " + cols + " columns of values, got " + row.size() + " columns of values");
        }
        for (int i = 1; i < cols; ++i) {
            if (row.get(i).rows() != row.get(i - 1).rows()) {
                throw new RuntimeException("all columns must have the same size");
            }
        }
        for (int i = 0; i < cols; ++i) {
            checkColumnType(i, row.get(i).getDataCategory(), row.get(i).getDataType());
        }
        if (row.get(partitionColumnIdx) instanceof AbstractVector) {
            return CHECK_RESULT_MULTI_ROWS;
        } else {
            return CHECK_RESULT_SINGLE_ROW;
        }
    }
    /**
     * Append a list of columns to the table.
     * @param row
     * @return number of rows appended.
     * @throws IOException
     */
    public int append(List<Entity> row) throws IOException {
        if (check(row) == CHECK_RESULT_SINGLE_ROW) {
            return appendSingle(row);
        } else { // CHECK_RESULT_MULTI_ROWS
            return appendBatch(row);
        }
    }

    public void shutdownThreadPool() {
        this.threadPool.shutdown();
    }
}
