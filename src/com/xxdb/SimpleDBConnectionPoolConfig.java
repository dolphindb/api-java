package com.xxdb;

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
}


