package com.xxdb.replicator;

import com.xxdb.comm.ErrorCodeInfo;

/**
 * Status information for a single stream replication target.
 * Tracks the number of rows inserted, dumped, and any error messages.
 * Extends ErrorCodeInfo to provide consistent error handling.
 */
public class ReplicatorStreamStatus extends ErrorCodeInfo {
    private long insertedRows;
    private long dumpedRows;

    /**
     * Creates a new ReplicatorStreamStatus with default values.
     */
    public ReplicatorStreamStatus() {
        super();
        this.insertedRows = 0;
        this.dumpedRows = 0;
    }

    /**
     * Creates a new ReplicatorStreamStatus with the specified values.
     *
     * @param insertedRows The total number of rows inserted into the queue
     * @param dumpedRows   The number of rows successfully written to the server
     * @param errorInfo    ErrorCodeInfo containing error code and message
     */
    public ReplicatorStreamStatus(long insertedRows, long dumpedRows, ErrorCodeInfo errorInfo) {
        super(errorInfo);
        this.insertedRows = insertedRows;
        this.dumpedRows = dumpedRows;
    }

    /**
     * Gets the total number of rows that have been inserted into the queue.
     *
     * @return The number of inserted rows
     */
    public long getInsertedRows() {
        return insertedRows;
    }

    /**
     * Sets the total number of rows inserted.
     *
     * @param insertedRows The number of inserted rows
     */
    public void setInsertedRows(long insertedRows) {
        this.insertedRows = insertedRows;
    }

    /**
     * Gets the number of rows that have been successfully written to the server.
     *
     * @return The number of dumped rows
     */
    public long getDumpedRows() {
        return dumpedRows;
    }

    /**
     * Sets the number of rows successfully written.
     *
     * @param dumpedRows The number of dumped rows
     */
    public void setDumpedRows(long dumpedRows) {
        this.dumpedRows = dumpedRows;
    }

    /**
     * Gets the number of rows waiting in the queue (not yet written).
     *
     * @return The number of pending rows
     */
    public long getPendingRows() {
        return insertedRows - dumpedRows;
    }

    /**
     * Sets error information from another ErrorCodeInfo.
     *
     * @param errorInfo The error information to copy
     */
    public void setError(ErrorCodeInfo errorInfo) {
        this.set(errorInfo);
    }

    @Override
    public String toString() {
        return String.format("ReplicatorStreamStatus{inserted=%d, dumped=%d, pending=%d, errorCode='%s', errorInfo='%s'}",
            insertedRows, dumpedRows, getPendingRows(), getErrorCode(), getErrorInfo());
    }
}