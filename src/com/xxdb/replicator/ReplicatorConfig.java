package com.xxdb.replicator;

import com.xxdb.comm.ConnectionState;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Vector;
import java.util.concurrent.TimeUnit;

/**
 * Configuration for StreamReplicator.
 * Uses builder pattern for flexible configuration.
 */
public class ReplicatorConfig {
    private int batchSize;
    private long batchInterval;
    private int maxRetry;
    private long retryInterval;
    private int[] compression;
    private DataCallback onDataDump;
    private StateCallback onConnectionStateChange;

    /**
     * Callback interface for data dump events.
     * Called after each batch write attempt (successful or failed).
     * After the callback returns, data is removed from the queue.
     */
    @FunctionalInterface
    public interface DataCallback {
        /**
         * Called when data is dumped to a host.
         *
         * @param hostLabel The label of the host
         * @param table     The table that was dumped
         * @return true to continue replication, false to stop
         */
        boolean onDump(String hostLabel, BasicTable table);
    }

    /**
     * Callback interface for connection state change events.
     */
    @FunctionalInterface
    public interface StateCallback {
        /**
         * Called when connection state changes.
         *
         * @param state     The new connection state
         * @param hostLabel The label of the host
         * @return true to continue, false to stop
         */
        boolean onStateChange(ConnectionState state, String hostLabel);
    }

    /**
     * Creates a new ReplicatorConfig with default values.
     * Default callback simply removes data from queue to prevent accumulation.
     */
    public ReplicatorConfig() {
        this.batchSize = 1;
        this.batchInterval = 0;
        this.maxRetry = 3;
        this.retryInterval = 1000;
        this.compression = null;
        // Default callback: remove data from queue regardless of success/failure
        this.onDataDump = (hostLabel, table) -> true;
        this.onConnectionStateChange = (state, hostLabel) -> true;
    }

    /**
     * Sets the batching parameters.
     *
     * @param batchSize The number of rows to accumulate before writing (must be > 0)
     * @param throttle  The maximum time in seconds to wait before writing, regardless of batch size (0 means no timeout)
     * @return This config instance for chaining
     */
    public ReplicatorConfig setBatching(int batchSize, float throttle) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than 0");
        }
        if (throttle < 0) {
            throw new IllegalArgumentException("throttle must be greater than or equal to 0");
        }
        this.batchSize = batchSize;
        this.batchInterval = (long)(throttle * 1000);
        return this;
    }

    /**
     * Sets the batching parameters with TimeUnit (alternative API).
     *
     * @param batchSize     The number of rows to accumulate before writing (must be > 0)
     * @param batchInterval The maximum time to wait before writing, regardless of batch size
     * @param unit          The time unit for batchInterval
     * @return This config instance for chaining
     */
    public ReplicatorConfig setBatching(int batchSize, long batchInterval, TimeUnit unit) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than 0");
        }
        if (batchInterval < 0) {
            throw new IllegalArgumentException("batchInterval cannot be negative");
        }
        this.batchSize = batchSize;
        this.batchInterval = unit.toMillis(batchInterval);
        return this;
    }

    /**
     * Sets the retry parameters for reconnection.
     *
     * @param maxRetry      The maximum number of retry attempts (0 means no retry, -1 means infinite retry)
     * @param retryInterval The interval between retry attempts
     * @param unit          The time unit for retryInterval
     * @return This config instance for chaining
     */
    public ReplicatorConfig setRetry(int maxRetry, long retryInterval, TimeUnit unit) {
        if (maxRetry < -1) {
            throw new IllegalArgumentException("maxRetry must be -1 (infinite), 0 (no retry), or positive");
        }
        if (retryInterval < 0) {
            throw new IllegalArgumentException("retryInterval cannot be negative");
        }
        this.maxRetry = maxRetry;
        this.retryInterval = unit.toMillis(retryInterval);
        return this;
    }

    /**
     * Sets the compression methods to use for each column.
     * Use constants from {@link Vector}: COMPRESS_LZ4, COMPRESS_DELTA.
     * Null or empty array means no compression.
     *
     * @param compressMethods Array of compression method codes (one per column)
     * @return This config instance for chaining
     */
    public ReplicatorConfig setCompression(int[] compressMethods) {
        if (compressMethods != null && compressMethods.length > 0) {
            // Validate compression methods
            for (int method : compressMethods) {
                if (method != Vector.COMPRESS_LZ4 && method != Vector.COMPRESS_DELTA) {
                    throw new IllegalArgumentException("Unsupported compress method: " + method +
                        ". Only COMPRESS_LZ4 (" + Vector.COMPRESS_LZ4 + ") and COMPRESS_DELTA (" +
                        Vector.COMPRESS_DELTA + ") are supported.");
                }
            }
            this.compression = new int[compressMethods.length];
            System.arraycopy(compressMethods, 0, this.compression, 0, compressMethods.length);
        } else {
            this.compression = null;
        }
        return this;
    }

    /**
     * Sets the callback to be invoked when data is dumped to a host.
     * The callback is triggered after each write attempt (success or failure).
     * Data is removed from queue after callback returns.
     *
     * @param callback The callback function
     * @return This config instance for chaining
     */
    public ReplicatorConfig onDataDump(DataCallback callback) {
        this.onDataDump = callback == null ? (hostLabel, table) -> true : callback;
        return this;
    }

    /**
     * Sets the callback to be invoked when connection state changes.
     *
     * @param callback The callback function
     * @return This config instance for chaining
     */
    public ReplicatorConfig onConnectionStateChange(StateCallback callback) {
        this.onConnectionStateChange = callback == null ? (state, hostLabel) -> true : callback;
        return this;
    }

    // Getters
    public int getBatchSize() {
        return batchSize;
    }

    public long getBatchInterval() {
        return batchInterval;
    }

    public int getMaxRetry() {
        return maxRetry;
    }

    public long getRetryInterval() {
        return retryInterval;
    }

    public int[] getCompression() {
        if (compression == null) {
            return null;
        }
        int[] copy = new int[compression.length];
        System.arraycopy(compression, 0, copy, 0, compression.length);
        return copy;
    }

    public DataCallback getOnDataDump() {
        return onDataDump;
    }

    public StateCallback getOnConnectionStateChange() {
        return onConnectionStateChange;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ReplicatorConfig{batchSize=").append(batchSize)
          .append(", batchInterval=").append(batchInterval)
          .append(", maxRetry=").append(maxRetry)
          .append(", retryInterval=").append(retryInterval)
          .append(", compression=");
        if (compression == null) {
            sb.append("null");
        } else {
            sb.append("[");
            for (int i = 0; i < compression.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(compression[i]);
            }
            sb.append("]");
        }
        sb.append("}");
        return sb.toString();
    }
}