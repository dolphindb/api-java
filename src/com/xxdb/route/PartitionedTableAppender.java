package com.xxdb.route;

import com.xxdb.DBConnection;
import com.xxdb.data.*;

import java.io.IOException;
import java.util.*;

/**
 * PartitionedTableAppender is a used to append rows to a partitioned table
 * across a cluster of DolphinDB instances.
 *
 * <pre>
 * {@code
 *
 * PartitionedTableAppender tableAppender = new PartitionedTableAppender("Trades", "192.168.1.25", 8848);
 * List<Entity> row = new ArrayList<>();
 * row.add(BasicInt(1));
 * row.add(BasicString('A'));
 * tableAppender.append(row);
 * }
 * </pre>
 */
public class PartitionedTableAppender {
    // alias to connection mapping
    private Map<String, DBConnection> connectionMap = new HashMap<>();
    private BasicDictionary tableInfo;
    private TableRouter router;
    private BasicString tableName;
    private int partitionColumnIdx;
    private int cols;
    /**
     *
     * @param tableName name of the shared table
     * @param host host
     * @param port port
     */
    public PartitionedTableAppender(String tableName, String host, int port) throws IOException {
        this.tableName = new BasicString(tableName);
        DBConnection conn = new DBConnection();
        try {
            conn.connect(host, port);
            tableInfo = (BasicDictionary) conn.run("schema(" + tableName+ ")");
            partitionColumnIdx = ((BasicInt) tableInfo.get(new BasicString("partitionColumnIndex"))).getInt();
            if (partitionColumnIdx == -1) {
                throw new RuntimeException("Table '" + tableName + "' is not partitioned");
            }
            BasicAnyVector locations = (BasicAnyVector) tableInfo.get(new BasicString("partitionSites"));
            int partitionType = ((BasicInt) tableInfo.get(new BasicString("partitionType"))).getInt();
            AbstractVector partitionSchema = (AbstractVector) tableInfo.get(new BasicString("partitionSchema"));
            router = TableRouterFacotry.createRouter(Entity.PARTITION_TYPE.values()[partitionType], partitionSchema, locations);
            BasicTable colDefs = ((BasicTable) tableInfo.get(new BasicString("colDefs")));
            this.cols = colDefs.getColumn(0).rows();
        } catch (IOException e) {
            throw e;
        } finally {
            conn.close();
        }
    }

    private DBConnection getConnection(Entity partitioningColumn) throws IOException{
        if (!(partitioningColumn instanceof  Scalar))
            throw new RuntimeException("partitioning column value must be a scalar");
        System.out.println(partitioningColumn.toString());
        String dest = router.route((Scalar) partitioningColumn);
        System.out.println(partitioningColumn.toString() + " => " + dest);
        DBConnection conn = connectionMap.get(dest);
        if (conn == null) {
            conn = new DBConnection();
            String[] destParts = dest.split(":");
            conn.connect(destParts[0], Integer.valueOf(destParts[1]));
            connectionMap.put(dest, conn);
        }

        return conn;
    }

    /**
     * Append a list of columns to the table.
     * @param row
     * @return number of rows appended.
     * @throws IOException
     */
    public int append(List<Entity> row) throws IOException {
        if (row.size() != cols) {
            throw new RuntimeException("expect " + cols + " columns of values, got " + row.size() + " columns of values");
        }
        DBConnection conn = getConnection(row.get(partitionColumnIdx));
        List<Entity> args = new ArrayList<>();
        args.add(tableName);
        args.addAll(row);
        BasicInt affected = (BasicInt)conn.run("tableInsert", args);
        return affected.getInt();
    }
}
