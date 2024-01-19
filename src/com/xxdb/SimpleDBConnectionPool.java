package com.xxdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleDBConnectionPool {
    private SimpleDBConnectionPoolImpl connectionPool;
    private String hostName;
    private int port;
    private String userId;
    private String password;
    private int initialPoolSize;
    private String initialScript;
    private boolean compress;
    private boolean useSSL;
    private boolean usePython;
    private boolean loadBalance;
    private boolean enableHighAvailability;
    private String[] highAvailabilitySites;
    private boolean reconnect = true;
    private static final Logger log = LoggerFactory.getLogger(DBConnection.class);

    public SimpleDBConnectionPool(SimpleDBConnectionPoolConfig simpleDBConnectionPoolConfig) {
        if (Objects.isNull(simpleDBConnectionPoolConfig.getHostName()))
            throw new RuntimeException("The host name can not be null.");
        this.hostName = simpleDBConnectionPoolConfig.getHostName();
        if (simpleDBConnectionPoolConfig.getPort() <= 0)
            throw new RuntimeException("The port needs to be greater than 0.");
        this.port = simpleDBConnectionPoolConfig.getPort();
        if (Objects.isNull(simpleDBConnectionPoolConfig.getUserId()))
            throw new RuntimeException("The user Id can not be null.");
        this.userId = simpleDBConnectionPoolConfig.getUserId();
        if (Objects.isNull(simpleDBConnectionPoolConfig.getPassword()))
            throw new RuntimeException("The password can not be null.");
        this.password = simpleDBConnectionPoolConfig.getPassword();
        if (simpleDBConnectionPoolConfig.getInitialPoolSize() <= 0)
            throw new RuntimeException("The number of connection pools needs to be greater than 0.");
        this.initialPoolSize = simpleDBConnectionPoolConfig.getInitialPoolSize();
        this.initialScript = simpleDBConnectionPoolConfig.getInitialScript();
        this.compress = simpleDBConnectionPoolConfig.isCompress();
        this.useSSL = simpleDBConnectionPoolConfig.isUseSSL();
        this.usePython = simpleDBConnectionPoolConfig.isUsePython();
        this.loadBalance = simpleDBConnectionPoolConfig.isLoadBalance();
        this.enableHighAvailability = simpleDBConnectionPoolConfig.isEnableHighAvailability();
        this.highAvailabilitySites = simpleDBConnectionPoolConfig.getHighAvailabilitySites();
    }

    public DBConnection getConnection() {
        if (isClosed())
            throw new RuntimeException("The connection pool has been closed.");
        else if (Objects.nonNull(connectionPool)) {
            return connectionPool.getConnection();
        } else {
            synchronized (this) {
                if (Objects.isNull(connectionPool)) {
                    connectionPool = new SimpleDBConnectionPoolImpl();
                }
            }
            return connectionPool.getConnection();
        }
    }

    public int getActiveConnections() {
        return connectionPool.getCount(false);
    }

    public int getIdleConnections() {
        return connectionPool.getCount(true);
    }

    public void close() {
        if (Objects.nonNull(connectionPool))
            connectionPool.close();
        else
            log.info("The connection pool is closed.");
    }

    public boolean isClosed() {
        if (Objects.nonNull(connectionPool))
            return connectionPool.isClosed();
        else
            return false;
    }

    protected class SimpleDBConnectionPoolImpl {
        private CopyOnWriteArrayList<PoolEntry> poolEntries;
        private AtomicBoolean isShutdown = new AtomicBoolean();

        SimpleDBConnectionPoolImpl() {
            ArrayList<PoolEntry> poolEntryArrayList = new ArrayList<>(initialPoolSize);
            try {
                for (int i = 0; i < initialPoolSize; i++) {
                    PoolEntry poolEntry = new PoolEntry(useSSL, compress, usePython, String.format("DolphinDBConnection_%d", i + 1));
                    if (!poolEntry.connect(hostName, port, userId, password, initialScript, enableHighAvailability, highAvailabilitySites, reconnect, loadBalance)){
                        log.error(String.format("Connection %s connect failure.", poolEntry.connectionName));
                    };
                    poolEntryArrayList.add(poolEntry);
                }
                poolEntries = new CopyOnWriteArrayList<>(poolEntryArrayList);
            } catch (Exception e) {
                log.error("Create connection pool failure, because " + e.getMessage());
            }
        }

        DBConnection getConnection() {
            for (PoolEntry poolEntry : poolEntries) {
                if (poolEntry.inUse.compareAndSet(false, true)) {
                    return poolEntry;
                }
            }
            log.error("All connections in the connection pool are currently in use.");
            return null;
        }

        int getCount(boolean isIdle) {
            int count = 0;
            for (PoolEntry poolEntry : this.poolEntries) {
                if (poolEntry.isIdle() == isIdle)
                    count++;
            }
            return count;
        }

        void close() {
            if (!this.isShutdown.getAndSet(true)) {
                log.info("Closing the connection pool......");
                for (PoolEntry poolEntry : poolEntries) {
                    poolEntry.release();
                }
                log.info("Closing the connection pool finished.");
            } else {
                log.info("The connection pool is closed.");
            }
        }

        boolean isClosed() {
            return this.isShutdown.get();
        }

    }

    public class PoolEntry extends DBConnection {
        AtomicBoolean inUse = new AtomicBoolean(false);
        String connectionName;

        public PoolEntry(boolean useSSL, boolean compress, boolean usePython, String connectionName) {
            super(false, useSSL, compress, usePython);
            this.connectionName = connectionName;
        }

        public String getConnectionName() {
            return connectionName;
        }

        public boolean isIdle() {
            return !this.inUse.get();
        }

        @Override
        public void close() {
            if (isBusy())
                log.error("Cannot release the connection,is running now.");
            else
                inUse.compareAndSet(true, false);
        }

        private void release() {
            super.close();
        }
    }
}

