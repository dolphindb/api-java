package com.xxdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Objects;

public class SimpleDBConnectionPoolConfig {
    private String hostName;
    private int port;
    private String userId;
    private String password;
    private int initialPoolSize;
    private String initialScript = null;
    private boolean compress = false;
    private boolean useSSL = false;
    private boolean usePython = false;
    private boolean loadBalance = false;
    private boolean enableHighAvailability = false;
    private String[] highAvailabilitySites = null;
    private static final Logger log = LoggerFactory.getLogger(DBConnection.class);

    public SimpleDBConnectionPoolConfig() {
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        if (port <= 0)
            throw new RuntimeException("The port should be positive.");
        this.port = port;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getInitialPoolSize() {
        return initialPoolSize;
    }

    public void setInitialPoolSize(int initialPoolSize) {
        if (initialPoolSize <= 0)
            throw new RuntimeException("The number of connection pools should be positive.");
        this.initialPoolSize = initialPoolSize;
    }

    public String getInitialScript() {
        return initialScript;
    }

    public void setInitialScript(String initialScript) {
        this.initialScript = initialScript;
    }

    public boolean isCompress() {
        return compress;
    }

    public void setCompress(boolean compress) {
        this.compress = compress;
    }

    public boolean isUseSSL() {
        return useSSL;
    }

    public void setUseSSL(boolean useSSL) {
        this.useSSL = useSSL;
    }

    public boolean isUsePython() {
        return usePython;
    }

    public void setUsePython(boolean usePython) {
        this.usePython = usePython;
    }

    public boolean isLoadBalance() {
        return loadBalance;
    }

    public void setLoadBalance(boolean loadBalance) {
        this.loadBalance = loadBalance;
    }

    public boolean isEnableHighAvailability() {
        return enableHighAvailability;
    }

    public void setEnableHighAvailability(boolean enableHighAvailability) {
        this.enableHighAvailability = enableHighAvailability;
    }

    public String[] getHighAvailabilitySites() {
        return highAvailabilitySites;
    }

    public void setHighAvailabilitySites(String[] highAvailabilitySites) {
        this.highAvailabilitySites = highAvailabilitySites;
    }

    public void validate() {
        hostName = getNullIfEmpty(hostName);
        if (Objects.isNull(hostName)) {
            hostName = "localhost";
            log.warn("HostName not set, use the default value 'localhost'");
        }
        if (port <= 0) {
            port = 8848;
            log.warn("invalid port, use the default value 8848.");
        }
        userId = getNullIfEmpty(userId);
        if (Objects.isNull(userId))
            log.warn("Logging in needs userId.");
        password = getNullIfEmpty(password);
        if (Objects.isNull(password))
            log.warn("Logging in needs password.");
        if (initialPoolSize <= 0)
            throw new RuntimeException("The number of connection pools should be positive.");
    }

    private static String getNullIfEmpty(String text) {
        return text == null ? null : (text.trim().isEmpty() ? null : text.trim());
    }
}


