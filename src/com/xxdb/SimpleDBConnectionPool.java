package com.xxdb;

import com.xxdb.data.BasicInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleDBConnectionPool {
    private String hostName;
    private int port;
    private String userId;
    private String password;

    /**
     * @deprecated Use {@link #minimumPoolSize} instead.
     */
    @Deprecated
    private int initialPoolSize;
    private int minimumPoolSize;
    private int maximumPoolSize;
    private int idleTimeout;

    private String initialScript;
    private boolean compress;
    private boolean useSSL;
    private boolean usePython;
    private boolean loadBalance;
    private boolean enableHighAvailability;
    private String[] highAvailabilitySites;
    private boolean reconnect = true;
    private int tryReconnectNums;

    private static final Logger log = LoggerFactory.getLogger(DBConnection.class);

    private final Queue<PoolEntry> poolEntries = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean isPoolShutdown = new AtomicBoolean(false);
    private volatile boolean isPoolRunning = true;
    private Thread cleanupIdleConnsThread = null;

    public SimpleDBConnectionPool(SimpleDBConnectionPoolConfig simpleDBConnectionPoolConfig) {
        simpleDBConnectionPoolConfig.validate();

        this.hostName = simpleDBConnectionPoolConfig.getHostName();
        this.port = simpleDBConnectionPoolConfig.getPort();
        this.userId = simpleDBConnectionPoolConfig.getUserId();
        this.password = simpleDBConnectionPoolConfig.getPassword();
        this.minimumPoolSize = simpleDBConnectionPoolConfig.getMinimumPoolSize();
        this.maximumPoolSize = simpleDBConnectionPoolConfig.getMaximumPoolSize();
        this.idleTimeout = simpleDBConnectionPoolConfig.getIdleTimeout();
        this.initialScript = simpleDBConnectionPoolConfig.getInitialScript();
        this.compress = simpleDBConnectionPoolConfig.isCompress();
        this.useSSL = simpleDBConnectionPoolConfig.isUseSSL();
        this.usePython = simpleDBConnectionPoolConfig.isUsePython();
        this.loadBalance = simpleDBConnectionPoolConfig.isLoadBalance();
        this.enableHighAvailability = simpleDBConnectionPoolConfig.isEnableHighAvailability();
        this.highAvailabilitySites = simpleDBConnectionPoolConfig.getHighAvailabilitySites();
        this.tryReconnectNums = simpleDBConnectionPoolConfig.getTryReconnectNums();

        initPool();
    }

    private void initPool() {
        for (int i = 0; i < minimumPoolSize; i++) {
            try {
                PoolEntry poolEntry = new PoolEntry(useSSL, compress, usePython, String.format("DolphinDBConnection_%d", i + 1));
                if (poolEntry.entryConnect(hostName, port, userId, password, initialScript, enableHighAvailability, highAvailabilitySites, reconnect, loadBalance, tryReconnectNums)) {
                    poolEntries.add(poolEntry);
                } else {
                    throw new RuntimeException(String.format("Connection %s connect failure.", poolEntry.connectionName));
                }
            } catch (Exception e) {
                throw new RuntimeException("Create connection pool failure, because " + e.getMessage(), e);
            }
        }

        // Start Cleanup Task Thread
        this.cleanupIdleConnsThread = new Thread(new Runnable() {
            @Override
            public void run() {
                runCleanupTask();
            }
        });
        // Set as Daemon Thread.
        this.cleanupIdleConnsThread.setDaemon(true);
        this.cleanupIdleConnsThread.start();

        // Add Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                close();
            }
        }));
    }

    /**
     * Get an Idle Connection from the Connection Pool.
     */
    public DBConnection getConnection() {
        for (PoolEntry poolEntry : poolEntries) {
            if (poolEntry.inUse.compareAndSet(false, true)) {
                return poolEntry;
            }
        }

        synchronized (this) {
            if (poolEntries.size() < maximumPoolSize) {
                try {
                    PoolEntry poolEntry = new PoolEntry(useSSL, compress, usePython, String.format("DolphinDBConnection_%d", poolEntries.size() + 1));
                    if (poolEntry.entryConnect(hostName, port, userId, password, initialScript, enableHighAvailability, highAvailabilitySites, reconnect, loadBalance, tryReconnectNums)) {
                        poolEntries.add(poolEntry);
                        if (poolEntry.inUse.compareAndSet(false, true)) {
                            return poolEntry;
                        }
                    } else {
                        log.error(String.format("Connection %s connect failure.", poolEntry.connectionName));
                    }
                } catch (IOException e) {
                    log.error("Failed to create new connection: " + e.getMessage());
                }
            }
        }

        throw new RuntimeException("No available idle connections.");
    }

    /**
     * Get the Number of Currently Active Connections
     */
    public int getActiveConnectionsCount() {
        if (isClosed())
            throw new RuntimeException("The connection pool has been closed.");
        return getActiveOrIdleCount(false);
    }

    /**
     * Get the Number of Currently Idle Connections
     */
    public int getIdleConnectionsCount() {
        if (isClosed())
            throw new RuntimeException("The connection pool has been closed.");
        return getActiveOrIdleCount(true);
    }

    /**
     * Get total connections count.
     */
    public int getTotalConnectionsCount() {
        if (isClosed())
            throw new RuntimeException("The connection pool has been closed.");
        return poolEntries.size();
    }

    /**
     * Method for Manually Cleaning Up Idle Connections.
     */
    public synchronized void manualCleanupIdleConnections() {
        int totalConnections = poolEntries.size();

        Iterator<PoolEntry> iterator = poolEntries.iterator();
        while (iterator.hasNext()) {
            PoolEntry poolEntry = iterator.next();
            if (poolEntry.isIdle()) {
                // Clean Up Only When the Current Number of Connections Is Greater Than the minimumPoolSize.
                if (totalConnections > minimumPoolSize) {
                    poolEntry.release();
                    iterator.remove();
                    // update current conns count.
                    totalConnections--;
                }
            }
        }
    }

    /**
     * Close the connection pool.
     */
    public void close() {
        isPoolRunning = false;
        cleanupIdleConnsThread.interrupt();
        try {
            cleanupIdleConnsThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (!this.isPoolShutdown.getAndSet(true)) {
            log.info("Closing the connection pool......");
            if (poolEntries.size() != 0) {
                for (PoolEntry poolEntry : poolEntries) {
                    poolEntry.release();
                }
            }
            log.info("Closing the connection pool finished.");
        } else {
            log.info("The connection pool is closed.");
        }
    }

    /**
     * Check If the Connection Pool Is Closed.
     */
    public boolean isClosed() {
        return this.isPoolShutdown.get();
    }

    class PoolEntry extends DBConnection {
        AtomicBoolean inUse = new AtomicBoolean(false);
        String connectionName;
        private long lastUsedTime;

        PoolEntry(boolean useSSL, boolean compress, boolean usePython, String connectionName) {
            super(false, useSSL, compress, usePython);
            this.connectionName = connectionName;
        }

        private boolean entryConnect(String hostName, int port, String userId, String password, String initialScript, boolean enableHighAvailability, String[] highAvailabilitySites, boolean reconnect, boolean enableLoadBalance, int tryReconnectNums) throws IOException {
            boolean isConnected = super.connect(hostName, port, userId, password, initialScript, enableHighAvailability, highAvailabilitySites, reconnect, enableLoadBalance, tryReconnectNums);
            if (isConnected) {
                this.lastUsedTime = System.currentTimeMillis();
            }

            return isConnected;
        }

        /**
         * Check If the Current Connection Is Idle. (Internal Use)
         */
        private boolean isIdle() {
            return !this.inUse.get();
        }

        private long getLastUsedTime() {
            return lastUsedTime;
        }

        @Override
        public void setLoadBalance(boolean loadBalance) {
            throw new RuntimeException("The loadBalance configuration of connection in connection pool can only be set in SimpleDBConnectionPoolConfig.");
        }

        @Override
        public boolean connect(String hostName, int port, String userId, String password, String initialScript, boolean enableHighAvailability, String[] highAvailabilitySites, boolean reconnect, boolean enableLoadBalance) throws IOException {
            throw new RuntimeException("The connection in connection pool can only connect by pool.");
        }

        @Override
        public void login(String userId, String password, boolean enableEncryption) throws IOException {
            throw new RuntimeException("The connection in connection pool can only login by pool.");
        }

        /**
         * Release current connection to pool.
         */
        @Override
        public void close() {
            if (isBusy())
                log.error("Cannot release the connection, is running now.");
            else {
                try {
                    BasicInt ret = (BasicInt) run("1+1", true);
                    if (!ret.isNull() && (ret.getInt() == 2)) {
                        inUse.compareAndSet(true, false);
                        lastUsedTime = System.currentTimeMillis();
                    }
                    else
                        log.error("Cannot release memory, release connection failure.");
                } catch (Exception e) {
                    log.error("Cannot release memory, because " + e.getMessage());
                }
            }
        }

        /**
         * Close connection. (Internal Use.)
         */
        private void release() {
            super.close();
        }
    }

    /**
     * Internal clean up Idle Connections Exceeding the Maximum Idle Time
     */
    private synchronized void internalCleanupIdleConnections() {
        long currentTime = System.currentTimeMillis();
        int totalConnections = poolEntries.size();

        Iterator<PoolEntry> iterator = poolEntries.iterator();
        while (iterator.hasNext()) {
            PoolEntry poolEntry = iterator.next();
            if (poolEntry.isIdle() && (currentTime - poolEntry.getLastUsedTime() > idleTimeout)) {
                // Clean Up Only When the Current Number of Connections Is Greater Than the minimumPoolSize.
                if (totalConnections > minimumPoolSize) {
                    poolEntry.release();
                    iterator.remove();
                    // update current conns count.
                    totalConnections--;
                }
            }
        }
    }

    private void runCleanupTask() {
        while (isPoolRunning) {
            try {
                internalCleanupIdleConnections();
                // Sleep 1 seconds after cleanup.
                Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.err.println("Failed to clean up idle connections: " + e.getMessage());
            }
        }
    }

    private int getActiveOrIdleCount(boolean isIdle) {
        int count = 0;
        for (PoolEntry poolEntry : this.poolEntries) {
            if (poolEntry.isIdle() == isIdle)
                count++;
        }
        return count;
    }
}

