package com.xxdb.replicator;

import com.xxdb.DBConnection;
import com.xxdb.comm.ConnectionState;
import com.xxdb.comm.ErrorCodeInfo;
import com.xxdb.data.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * StreamReplicator provides multi-active asynchronous writing to multiple DolphinDB nodes.
 * It creates a writing thread for each target node and replicates data to all nodes.
 * <p>
 * This class is designed for high-availability scenarios where the same data needs to be
 * written to multiple DolphinDB nodes simultaneously for real-time stream computing.
 * </p>
 */
public class StreamReplicator implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(StreamReplicator.class);
    private static final int MAX_VECTOR_SIZE = 65535;

    private final List<HostInfo> hosts;
    private final String tableName;
    private final ReplicatorConfig config;
    private final List<WriterThread> writerThreads;
    private final SharedDataQueue sharedDataQueue;
    private final ReentrantLock insertLock;
    private volatile boolean isClosed;

    private List<Entity.DATA_TYPE> columnTypes;
    private List<Integer> columnExtras;
    private int columnCount;

    private List<Vector> currentBatch;
    private int currentBatchSize;

    /**
     * Creates a new StreamReplicator.
     *
     * @param hosts     List of target host information
     * @param tableName The name of the table to write to
     * @param config    Configuration for the replicator
     * @throws IOException If connection to any host fails during initialization
     */
    public StreamReplicator(List<HostInfo> hosts, String tableName, ReplicatorConfig config) throws IOException {
        if (hosts == null || hosts.isEmpty()) {
            throw new IllegalArgumentException("Hosts list cannot be null or empty");
        }
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
        if (config == null) {
            throw new IllegalArgumentException("Config cannot be null");
        }

        this.hosts = new ArrayList<>(hosts);
        this.tableName = tableName;
        this.config = config;
        this.writerThreads = new ArrayList<>();
        this.sharedDataQueue = new SharedDataQueue();
        this.insertLock = new ReentrantLock();
        this.isClosed = false;
        this.currentBatchSize = 0;

        // Initialize and start writer threads for each host
        initializeWriterThreads();
    }

    /**
     * Creates a new StreamReplicator with default configuration.
     *
     * @param hosts     List of target host information
     * @param tableName The name of the table to write to
     * @throws IOException If connection to any host fails during initialization
     */
    public StreamReplicator(List<HostInfo> hosts, String tableName) throws IOException {
        this(hosts, tableName, new ReplicatorConfig());
    }

    /**
     * Initializes writer threads for all hosts.
     */
    private void initializeWriterThreads() throws IOException {
        for (HostInfo host : hosts) {
            WriterThread thread = new WriterThread(host);
            writerThreads.add(thread);
            thread.start();
        }

        // Wait for at least one thread to initialize and get table schema
        boolean initialized = false;
        long startTime = System.currentTimeMillis();
        long timeout = 30000; // 30 seconds timeout

        while (!initialized && System.currentTimeMillis() - startTime < timeout) {
            for (WriterThread thread : writerThreads) {
                if (thread.isInitialized()) {
                    this.columnTypes = thread.getColumnTypes();
                    this.columnExtras = thread.getColumnExtras();
                    this.columnCount = columnTypes.size();
                    initialized = true;
                    break;
                }
            }
            if (!initialized) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for initialization", e);
                }
            }
        }

        if (!initialized) {
            throw new IOException("Failed to initialize: no connection could retrieve table schema within timeout");
        }

        // Validate compression array length if provided
        int[] compression = config.getCompression();
        if (compression != null && compression.length > 0 && compression.length != columnCount) {
            throw new IOException(String.format(
                "Compression array length (%d) does not match column count (%d)",
                compression.length, columnCount));
        }

        logger.info("StreamReplicator initialized for table '{}' with {} hosts and {} columns",
            tableName, hosts.size(), columnCount);
    }

    /**
     * Inserts a row of data into all target nodes using shared queue (memory-efficient).
     *
     * @param args The column values for the row (must match table schema)
     * @return ErrorCodeInfo indicating success or failure
     */
    public ErrorCodeInfo insert(Object... args) {
        if (isClosed) {
            return new ErrorCodeInfo(ErrorCodeInfo.Code.EC_DestroyedObject,
                "StreamReplicator has been closed");
        }

        if (args.length != columnCount) {
            return new ErrorCodeInfo(ErrorCodeInfo.Code.EC_InvalidParameter,
                String.format("Column count mismatch: expected %d columns, got %d", columnCount, args.length));
        }

        insertLock.lock();
        try {
            // Initialize current batch if needed
            if (currentBatch == null) {
                currentBatch = createVectorList();
            }

            // Convert objects to Entities and append to current batch
            for (int i = 0; i < args.length; i++) {
                try {
                    Entity.DATA_TYPE dataType = columnTypes.get(i);
                    int extra = columnExtras.get(i);
                    Entity entity;
                    if (args[i] instanceof Entity) {
                        entity = (Entity) args[i];
                    } else {
                        try {
                            entity = BasicEntityFactory.createScalar(dataType, args[i], extra);
                        } catch (Exception e) {
                            String errorMsg = "Invalid object error when create scalar for column " + i + ": " + e.getMessage();
                            return new ErrorCodeInfo(ErrorCodeInfo.Code.EC_InvalidObject, errorMsg);
                        }

                        if (entity == null) {
                            return new ErrorCodeInfo(ErrorCodeInfo.Code.EC_InvalidObject,
                                "Data conversion error for column " + i + ": " + dataType);
                        }
                    }

                    Vector targetVector = currentBatch.get(i);

                    if (entity instanceof Scalar) {
                        targetVector.Append((Scalar) entity);
                    } else if (entity instanceof Vector) {
                        targetVector.Append((Vector) entity);
                    } else {
                        throw new IllegalArgumentException("Unsupported entity type: " + entity.getClass());
                    }
                } catch (Exception e) {
                    return new ErrorCodeInfo(ErrorCodeInfo.Code.EC_InvalidColumnType,
                        String.format("Failed to convert column %d: %s", i, e.getMessage()));
                }
            }

            currentBatchSize++;

            // Check if we should flush to shared queue
            if (currentBatchSize >= config.getBatchSize() || currentBatch.get(0).rows() >= MAX_VECTOR_SIZE) {
                flushCurrentBatch();
            }

            return new ErrorCodeInfo(); // Success
        } finally {
            insertLock.unlock();
        }
    }

    /**
     * Creates a new vector list based on table schema.
     */
    private List<Vector> createVectorList() {
        List<Vector> vectors = new ArrayList<>();
        for (int i = 0; i < columnTypes.size(); i++) {
            Entity.DATA_TYPE type = columnTypes.get(i);
            int extra = columnExtras.get(i);
            Vector vector;

            if (type == Entity.DATA_TYPE.DT_ANY) {
                vector = new BasicAnyVector(0);
            } else if (type.getValue() >= 64) { // Array type
                vector = new BasicArrayVector(type, 1, extra);
            } else {
                vector = BasicEntityFactory.instance().createVectorWithDefaultValue(type, 0, extra);
            }
            vectors.add(vector);
        }
        return vectors;
    }

    /**
     * Flushes the current batch to the shared queue.
     */
    private void flushCurrentBatch() {
        if (currentBatch != null && currentBatchSize > 0) {
            sharedDataQueue.addBatch(currentBatch);
            currentBatch = null;
            currentBatchSize = 0;
        }
    }

    /**
     * Gets the current status of the replicator.
     *
     * @return The current status including per-host statistics
     */
    public ReplicatorStatus getStatus() {
        Map<String, ReplicatorStreamStatus> hostStatuses = new HashMap<>();
        long totalRows = 0;

        for (WriterThread thread : writerThreads) {
            ReplicatorStreamStatus status = thread.getStatus();
            hostStatuses.put(thread.getHostLabel(), status);
            totalRows = Math.max(totalRows, status.getInsertedRows());
        }

        return new ReplicatorStatus(totalRows, hostStatuses);
    }

    /**
     * Waits for all pending data to be written to hosts.
     * For disconnected hosts, data is processed through the callback.
     * Does not accept new insert calls after this method is called.
     */
    public void waitForThreadCompletion() {
        if (isClosed) {
            return;
        }

        // Flush any remaining data in current batch before closing
        insertLock.lock();
        try {
            flushCurrentBatch();
        } finally {
            insertLock.unlock();
        }

        isClosed = true; // Prevent new inserts

        // Signal all threads to exit (after processing remaining data)
        for (WriterThread thread : writerThreads) {
            thread.shutdown();
        }

        // Wait for all threads to finish processing
        for (WriterThread thread : writerThreads) {
            try {
                thread.join(30000); // 30 second timeout for completion
                if (thread.isAlive()) {
                    logger.warn("Writer thread for host '{}' did not complete within timeout", thread.getHostLabel());
                    thread.interrupt();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Interrupted while waiting for thread completion", e);
            }
        }

        logger.info("All writer threads completed for table '{}'", tableName);
    }

    /**
     * Waits for all pending data to be written and closes all connections.
     * Internally calls waitForThreadCompletion() if not already called.
     */
    @Override
    public void close() {
        if (isClosed) {
            // Already waited or closed
            return;
        }

        logger.info("Closing StreamReplicator for table '{}'", tableName);

        // Wait for threads to complete
        waitForThreadCompletion();
    }

    /**
     * Writer thread for a single host (now using shared queue).
     */
    private class WriterThread extends Thread {
        private final HostInfo hostInfo;
        private final ReplicatorStreamStatus status;

        private DBConnection connection;
        private volatile boolean shouldExit;
        private volatile boolean initialized;
        private List<Entity.DATA_TYPE> columnTypes;
        private List<Integer> columnExtras;
        private ConnectionState connectionState;
        private long lastProcessedSequenceId;

        public WriterThread(HostInfo hostInfo) {
            super("StreamReplicator-" + hostInfo.getLabel());
            this.hostInfo = hostInfo;
            this.status = new ReplicatorStreamStatus();
            this.shouldExit = false;
            this.initialized = false;
            this.connectionState = ConnectionState.Initializing;
            this.lastProcessedSequenceId = -1; // Start from the beginning
            setDaemon(false);
        }

        public boolean isInitialized() {
            return initialized;
        }

        public List<Entity.DATA_TYPE> getColumnTypes() {
            return columnTypes;
        }

        public List<Integer> getColumnExtras() {
            return columnExtras;
        }

        public String getHostLabel() {
            return hostInfo.getLabel();
        }

        public ReplicatorStreamStatus getStatus() {
            long insertedRows = sharedDataQueue.getTotalInsertedRows();
            return new ReplicatorStreamStatus(
                insertedRows,
                status.getDumpedRows(),
                status
            );
        }

        public void shutdown() {
            shouldExit = true;
            sharedDataQueue.wakeupAll();
        }

        @Override
        public void run() {
            try {
                // Initial connection
                if (!connect()) {
                    logger.error("Failed to establish initial connection to host '{}'", hostInfo.getLabel());
                    return;
                }

                // Main processing loop - read from shared queue
                while (!shouldExit) {
                    SharedBatchData sharedBatch = sharedDataQueue.getNextBatch(lastProcessedSequenceId);

                    if (sharedBatch != null) {
                        // Process the batch
                        writeBatch(sharedBatch.getBatch());

                        // Mark as processed and update sequence ID
                        sharedDataQueue.markBatchProcessed(sharedBatch.getSequenceId());
                        lastProcessedSequenceId = sharedBatch.getSequenceId();
                    } else {
                        // No data available, wait for new data
                        try {
                            long waitTime = config.getBatchInterval() > 0 ? config.getBatchInterval() : 1000;
                            sharedDataQueue.waitForData(waitTime);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            shouldExit = true;
                            break;
                        }
                    }
                }

                // Final processing - process remaining batches
                SharedBatchData remainingBatch;
                while ((remainingBatch = sharedDataQueue.getNextBatch(lastProcessedSequenceId)) != null) {
                    writeBatch(remainingBatch.getBatch());
                    sharedDataQueue.markBatchProcessed(remainingBatch.getSequenceId());
                    lastProcessedSequenceId = remainingBatch.getSequenceId();
                }

            } finally {
                if (connection != null) {
                    connection.close();
                }
                logger.info("Writer thread stopped for host '{}'", hostInfo.getLabel());
            }
        }

        private boolean connect() {
            try {
                updateConnectionState(ConnectionState.Initializing);

                connection = new DBConnection(false, false, false);
                boolean connected = connection.connect(
                    hostInfo.getHost(),
                    hostInfo.getPort(),
                    hostInfo.getUserId(),
                    hostInfo.getPassword()
                );

                if (!connected) {
                    updateConnectionState(ConnectionState.Terminated);
                    return false;
                }

                // Get table schema
                if (!initialized) {
                    retrieveTableSchema();
                    initialized = true;
                }

                updateConnectionState(ConnectionState.Connected);
                status.clearError();
                return true;

            } catch (Exception e) {
                logger.error("Failed to connect to host '{}': {}", hostInfo.getLabel(), e.getMessage());
                ErrorCodeInfo errorInfo = new ErrorCodeInfo(ErrorCodeInfo.Code.EC_Server,
                    "Connection failed: " + e.getMessage());
                status.setError(errorInfo);
                updateConnectionState(ConnectionState.Terminated);
                return false;
            }
        }


        private void retrieveTableSchema() throws IOException {
            try {
                // Get table schema using schema() function
                String script = String.format("schema(%s)", tableName);
                BasicDictionary schema = (BasicDictionary) connection.run(script);

                // Extract column types and extras
                BasicTable colDefs = (BasicTable) schema.get(new BasicString("colDefs"));
                BasicIntVector colDefsTypeInt = (BasicIntVector) colDefs.getColumn("typeInt");
                BasicIntVector colExtra = (BasicIntVector) colDefs.getColumn("extra");

                columnTypes = new ArrayList<>();
                columnExtras = new ArrayList<>();
                for (int i = 0; i < colDefsTypeInt.rows(); i++) {
                    // Use integer type code instead of string mapping
                    int typeInt = colDefsTypeInt.getInt(i);
                    Entity.DATA_TYPE type = Entity.DATA_TYPE.valueOf(typeInt);
                    if (config.getCompression() != null && !AbstractVector.checkCompressedMethod(Entity.DATA_TYPE.valueOf(typeInt), config.getCompression()[i])) {
                        throw new RuntimeException("Compression Failed: only support integral and temporal data, not support " + type);
                    }
                    columnTypes.add(type);
                    int extra = -1;
                    if (colExtra != null) {
                        extra = colExtra.getInt(i);
                    }
                    columnExtras.add(extra);
                }

                logger.debug("Retrieved schema for table '{}' from host '{}': {} columns",
                    tableName, hostInfo.getLabel(), columnTypes.size());

            } catch (Exception e) {
                throw new IOException("Failed to retrieve table schema: " + e.getMessage(), e);
            }
        }

        private void writeBatch(List<Vector> batch) {
            if (batch == null || batch.isEmpty()) {
                return;
            }

            int rowCount = batch.get(0).rows();
            if (rowCount == 0) {
                return;
            }

            // Create table from vectors
            List<String> colNames = new ArrayList<>();
            for (int i = 0; i < batch.size(); i++) {
                colNames.add("col" + i);
            }
            BasicTable table = new BasicTable(colNames, batch);

            // Apply compression if configured
            int[] compression = config.getCompression();
            if (compression != null && compression.length > 0) {
                try {
                    table.setColumnCompressTypes(compression);
                } catch (Exception e) {
                    logger.error("Failed to set compression for host '{}': {}", hostInfo.getLabel(), e.getMessage());
                }
            }

            // Retry loop for writing data (similar to C++ implementation)
            boolean success = false;
            int maxRetry = config.getMaxRetry();
            int retry = 0;

            while (!success && (maxRetry == -1 || retry <= maxRetry)) {
                try {
                    // Check and restore connection if needed
                    if (connection == null || !connection.isConnected()) {
                        if (connectionState != ConnectionState.Reconnecting) {
                            updateConnectionState(ConnectionState.Reconnecting);
                        }

                        // Try to reconnect
                        connection = new DBConnection(false, false, false);
                        boolean connected = connection.connect(
                            hostInfo.getHost(),
                            hostInfo.getPort(),
                            hostInfo.getUserId(),
                            hostInfo.getPassword()
                        );

                        if (!connected) {
                            throw new IOException("Failed to reconnect");
                        }

                        logger.info("Reconnected to host '{}'", hostInfo.getLabel());
                    }

                    // Execute insert
                    String script = String.format("tableInsert{%s}", tableName);
                    List<Entity> args = new ArrayList<>();
                    args.add(table);
                    connection.run(script, args);

                    // Success!
                    success = true;

                    // Update status on success (insertedRows, not dumpedRows)
                    status.setDumpedRows(status.getDumpedRows() + rowCount);
                    status.clearError();

                    // Update connection state if was reconnecting
                    if (connectionState == ConnectionState.Reconnecting) {
                        updateConnectionState(ConnectionState.Connected);
                    }

                    logger.debug("Successfully wrote {} rows to host '{}'", rowCount, hostInfo.getLabel());

                } catch (Exception e) {
                    // Write failed
                    String errorMsg = "Failed to write batch to host '" + hostInfo.getLabel() +
                                    "' (attempt " + (retry + 1) + "/" +
                                    (maxRetry == -1 ? "∞" : (maxRetry + 1)) + "): " + e.getMessage();
                    logger.error(errorMsg);

                    ErrorCodeInfo errorInfo = new ErrorCodeInfo(ErrorCodeInfo.Code.EC_Server, errorMsg);
                    status.setError(errorInfo);

                    // Update connection state
                    if (connectionState != ConnectionState.Reconnecting) {
                        updateConnectionState(ConnectionState.Reconnecting);
                    }

                    // If this is the last retry attempt
                    if (maxRetry != -1 && retry >= maxRetry) {
                        logger.error("Max retry attempts reached for host '{}', data will be passed to callback",
                                   hostInfo.getLabel());

                        // Mark as dumped (failed)
                        status.setDumpedRows(status.getDumpedRows() + rowCount);

                        // Invoke callback for failed data (only on final failure)
                        try {
                            boolean continueReplication = config.getOnDataDump().onDump(
                                hostInfo.getLabel(),
                                table
                            );

                            if (!continueReplication) {
                                logger.warn("Callback requested to stop replication for host '{}'",
                                          hostInfo.getLabel());
                                shouldExit = true;
                            }
                        } catch (Exception callbackEx) {
                            logger.error("Error in data dump callback for host '{}': {}",
                                       hostInfo.getLabel(), callbackEx.getMessage());
                        }

                        break; // Exit retry loop
                    } else {
                        // Sleep before retry
                        try {
                            Thread.sleep(config.getRetryInterval());
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            logger.error("Interrupted during retry sleep for host '{}'", hostInfo.getLabel());
                            break;
                        }
                    }

                    retry++;

                    // Close bad connection before retry
                    if (connection != null) {
                        try {
                            connection.close();
                        } catch (Exception closeEx) {
                            logger.debug("Error closing connection: {}", closeEx.getMessage());
                        }
                        connection = null;
                    }
                }
            }

            // Data is removed from queue after processing (already dequeued)
        }


        private void updateConnectionState(ConnectionState newState) {
            ConnectionState oldState = this.connectionState;
            this.connectionState = newState;
            if (oldState != newState) {
                config.getOnConnectionStateChange().onStateChange(newState, hostInfo.getLabel());
                logger.info("Connection state changed for host '{}': {} -> {}",
                    hostInfo.getLabel(), oldState, newState);
            }
        }
    }

    /**
     * Represents a shared batch of data with reference counting.
     * Memory is released only when all writer threads have processed the batch.
     */
    private static class SharedBatchData {
        private final List<Vector> batch;
        private final AtomicInteger refCount;
        private final long sequenceId;

        public SharedBatchData(List<Vector> batch, int initialRefCount, long sequenceId) {
            this.batch = batch;
            this.refCount = new AtomicInteger(initialRefCount);
            this.sequenceId = sequenceId;
        }

        public List<Vector> getBatch() {
            return batch;
        }

        public long getSequenceId() {
            return sequenceId;
        }

        /**
         * Decrements the reference count and returns true if this was the last reference.
         */
        public boolean decrementAndCheckIfLast() {
            return refCount.decrementAndGet() == 0;
        }

        public int getRefCount() {
            return refCount.get();
        }
    }

    /**
     * Shared data queue that manages batches with reference counting.
     * Ensures data is only stored once and released when all threads have processed it.
     */
    private class SharedDataQueue {
        private final List<SharedBatchData> queue;
        private final Object lock;
        private long sequenceCounter;
        private long totalInsertedRows;

        public SharedDataQueue() {
            this.queue = new ArrayList<>();
            this.lock = new Object();
            this.sequenceCounter = 0;
            this.totalInsertedRows = 0;
        }

        /**
         * Adds a batch to the shared queue with reference count = number of writer threads.
         */
        public void addBatch(List<Vector> batch) {
            synchronized (lock) {
                SharedBatchData sharedBatch = new SharedBatchData(batch, writerThreads.size(), sequenceCounter++);
                queue.add(sharedBatch);
                totalInsertedRows += batch.get(0).rows();
                lock.notifyAll();
            }
        }

        /**
         * Gets the next batch for a specific thread starting from the given sequence ID.
         * Returns null if no new batches are available.
         */
        public SharedBatchData getNextBatch(long lastProcessedSequenceId) {
            synchronized (lock) {
                for (SharedBatchData batchData : queue) {
                    if (batchData.getSequenceId() > lastProcessedSequenceId) {
                        return batchData;
                    }
                }
                return null;
            }
        }

        /**
         * Marks a batch as processed by one thread.
         * If all threads have processed it, removes it from the queue (releases memory).
         */
        public void markBatchProcessed(long sequenceId) {
            synchronized (lock) {
                for (int i = 0; i < queue.size(); i++) {
                    SharedBatchData batchData = queue.get(i);
                    if (batchData.getSequenceId() == sequenceId) {
                        if (batchData.decrementAndCheckIfLast()) {
                            // All threads have processed this batch, remove it
                            queue.remove(i);
                            logger.debug("Released shared batch {} (all threads processed)", sequenceId);
                        }
                        break;
                    }
                }
            }
        }

        /**
         * Waits for new batches to be available or until timeout.
         */
        public void waitForData(long timeoutMs) throws InterruptedException {
            synchronized (lock) {
                lock.wait(timeoutMs);
            }
        }

        /**
         * Gets the total number of inserted rows across all batches.
         */
        public long getTotalInsertedRows() {
            synchronized (lock) {
                return totalInsertedRows;
            }
        }


        /**
         * Wakes up all waiting threads (used for shutdown).
         */
        public void wakeupAll() {
            synchronized (lock) {
                lock.notifyAll();
            }
        }
    }

    /**
     * Utility class for converting Java objects to DolphinDB entities.
     */
    public static Entity toEntity(Object obj, Entity.DATA_TYPE expectedType, int extra) {
        if (obj == null) {
            Scalar scalar = BasicEntityFactory.instance().createScalarWithDefaultValue(expectedType);
            scalar.setNull();
            return scalar;
        }

        if (obj instanceof Entity) {
            return (Entity) obj;
        }

        // Use BasicEntityFactory static method to create appropriate scalar
        try {
            return BasicEntityFactory.createScalar(expectedType, obj, extra);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert object to Entity: " + e.getMessage(), e);
        }
    }
}