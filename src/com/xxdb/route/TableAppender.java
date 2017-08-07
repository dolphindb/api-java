package com.xxdb.route;

import com.xxdb.DBConnection;
import com.xxdb.data.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TableAppender is a used to append rows to a normal table.
 *
 * <pre>
 * {@code
 *
 * TableAppender tableAppender = new PartitionedTableAppender("Trades", "192.168.1.25", 8848);
 * List<Entity> row = new ArrayList<>();
 * row.add(BasicInt(1));
 * row.add(BasicString('A'));
 * tableAppender.append(row);
 * }
 * </pre>
 */
public class TableAppender {
    // alias to connection mapping
    private DBConnection conn;
    private BasicDictionary tableInfo;
    private BasicString tableName;
    private int cols;
    /**
     *
     * @param tableName name of the shared table
     * @param host host
     * @param port port
     */
    public TableAppender(String tableName, String host, int port) throws IOException {
        this.tableName = new BasicString(tableName);
        this.conn = new DBConnection();
        try {
            this.conn.connect(host, port);
            tableInfo = (BasicDictionary) conn.run("schema(" + tableName+ ")");
            int partitionColumnIdx = ((BasicInt) tableInfo.get(new BasicString("partitionColumnIndex"))).getInt();
            if (partitionColumnIdx == -1) {
                throw new RuntimeException("Table '" + tableName + "' is partitioned");
            }
            BasicTable colDefs = ((BasicTable) tableInfo.get(new BasicString("colDefs")));
            this.cols = colDefs.getColumn(0).rows();
        } catch (IOException e) {
            throw e;
        }
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
        List<Entity> args = new ArrayList<>();
        args.add(tableName);
        args.addAll(row);
        BasicInt affected = (BasicInt)conn.run("tableInsert", args);
        return affected.getInt();
    }
}
