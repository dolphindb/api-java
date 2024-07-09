package com.xxdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Objects;

public class SimpleDBConnectionPoolConfig {
    private String hostName;
    private int port;
    private String userId;
    private String password;

    /**
     * @deprecated Use {@link #minimumPoolSize} instead.
     */
    @Deprecated
    private int initialPoolSize = -1;
    private int minimumPoolSize = 5;
    private int maximumPoolSize = 5;
    private int idleTimeout;

    private String initialScript = null;
    private boolean compress = false;
    private boolean useSSL = false;
    private boolean usePython = false;
    private boolean loadBalance = false;
    private boolean enableHighAvailability = false;
    private String[] highAvailabilitySites = null;
    private int tryReconnectNums = -1;

    private boolean isMinimumPoolSizeUserSet = false;
    private boolean isMaximumPoolSizeUserSet = false;

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

    @Deprecated
    public int getInitialPoolSize() {
        return initialPoolSize;
    }

    /**
     * @deprecated This method is deprecated and should not be used.
     * Use {@link #setMinimumPoolSize(int)} ()} instead.
     */
    @Deprecated
    public void setInitialPoolSize(int initialPoolSize) {
        if (initialPoolSize <= 0)
            throw new RuntimeException("The number of connection pools should be positive.");
        this.initialPoolSize = initialPoolSize;
    }

    public void setMinimumPoolSize(int minimumPoolSize) {
        this.minimumPoolSize = minimumPoolSize;
        this.isMinimumPoolSizeUserSet = true;
    }

    public int getMinimumPoolSize() {
        return minimumPoolSize;
    }

    public void setMaximumPoolSize(int maximumPoolSize) {
        this.maximumPoolSize = maximumPoolSize;
        this.isMaximumPoolSizeUserSet = true;
    }

    public int getMaximumPoolSize() {
        return maximumPoolSize;
    }

    public void setIdleTimeout(int idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public int getIdleTimeout() {
        return idleTimeout;
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

    public void setTryReconnectNums(int tryReconnectNums) {
        this.tryReconnectNums = tryReconnectNums;
    }

    public int getTryReconnectNums() {
        return tryReconnectNums;
    }

    /**
     * add param check when pool init start.
     */
    protected void validate() {
        hostName = getNullIfEmpty(hostName);
        if (Objects.isNull(hostName)) {
            hostName = "localhost";
            log.warn("The param hostName not set, will use the default value 'localhost'");
        }

        if (!checkHostNameValid(hostName))
            throw new RuntimeException(String.format("Invalid hostName: %s", hostName));

        if (port <= 0) {
            port = 8848;
            log.warn("Invalid port, will use the default value 8848.");
        }

        userId = getNullIfEmpty(userId);
        if (Objects.isNull(userId)){
            userId = "";
            log.warn("Login needs userId.");
        }

        password = getNullIfEmpty(password);
        if (Objects.isNull(password)){
            password = "";
            log.warn("Login needs password.");
        }

        if (initialPoolSize > 0 && !isMinimumPoolSizeUserSet && !isMaximumPoolSizeUserSet) {
            // 兼容旧版逻辑（只填init，不填mini、max的场景）：将最小、最大设置为 initialPoolSize 的值
            minimumPoolSize = initialPoolSize;
            maximumPoolSize = initialPoolSize;
        }

        if (initialPoolSize < 0) {
            // 新逻辑（不填init，如果mini、max也不填，这俩参数给默认值，填了照常取值但是要做判断）：mini、max 不填的话，自动设置默认值
            if (minimumPoolSize <= 0) {
                minimumPoolSize = 5;
                log.warn("The param 'minimumIdle' cannot less than or equal to 0, will use the default value 5.");
            }

            if (maximumPoolSize <= 0) {
                maximumPoolSize = 5;
                log.warn("The param 'maximumPoolSize' cannot less than or equal to 0, will use the default value 5.");
            }

            if (maximumPoolSize < minimumPoolSize) {
                    maximumPoolSize = minimumPoolSize;
                    log.warn("The param 'maximumPoolSize' cannot less than 'minimumIdle', 'maximumPoolSize' will be set equal to 'minimumIdle' value.");
            }
        } else {
            if (minimumPoolSize <= 0) {
                minimumPoolSize = 5;
                log.warn("The param 'minimumIdle' cannot less than or equal to 0, will use the default value 5.");
            }

            if (maximumPoolSize <= 0) {
                maximumPoolSize = 5;
                log.warn("The param 'maximumPoolSize' cannot less than or equal to 0, will use the default value 5.");
            }

            if (maximumPoolSize < minimumPoolSize) {
                    maximumPoolSize = minimumPoolSize;
                    log.warn("The param 'maximumPoolSize' cannot less than 'minimumIdle', 'maximumPoolSize' will be set equal to 'minimumIdle' value.");
            }
        }

        if (idleTimeout < 10000) {
            idleTimeout = 600000;
            log.warn("The param 'idleTimeout' cannot less than 10000ms， will use the default value 600000ms(10min)");
        }
    }

    private static String getNullIfEmpty(String text) {
        return text == null ? null : (text.trim().isEmpty() ? null : text.trim());
    }

    private static boolean checkHostNameValid(String hostName) {
        return hostName.equals("localhost") ||
                isIPV4(hostName) ||
                isIPV6(hostName) ||
                isDomain(hostName);
    }

    private static boolean isDomain(String hostName) {
        String regex = "^([a-z0-9]+(-[a-z0-9]+)*\\.)+[a-z]{2,}$";
        return hostName.matches(regex);
    }

    private static boolean isIPV4(String hostName) {
        String regex = "^(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})$";
        if (!hostName.matches(regex)) {
            return false;
        }
        String[] parts = hostName.split("\\.");
        try {
            for (String segment : parts) {
                if (Integer.parseInt(segment) > 255) {
                    return false;
                }
            }
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    private static boolean isIPV6(String hostName) {
        String regex = "(^((([0-9A-Fa-f]{1,4}:){7}(([0-9A-Fa-f]{1,4}){1}|:))"
                + "|(([0-9A-Fa-f]{1,4}:){6}((:[0-9A-Fa-f]{1,4}){1}|"
                + "((22[0-3]|2[0-1][0-9]|[0-1][0-9][0-9]|"
                + "([0-9]){1,2})([.](25[0-5]|2[0-4][0-9]|"
                + "[0-1][0-9][0-9]|([0-9]){1,2})){3})|:))|"
                + "(([0-9A-Fa-f]{1,4}:){5}((:[0-9A-Fa-f]{1,4}){1,2}|"
                + ":((22[0-3]|2[0-1][0-9]|[0-1][0-9][0-9]|"
                + "([0-9]){1,2})([.](25[0-5]|2[0-4][0-9]|"
                + "[0-1][0-9][0-9]|([0-9]){1,2})){3})|:))|"
                + "(([0-9A-Fa-f]{1,4}:){4}((:[0-9A-Fa-f]{1,4}){1,3}"
                + "|:((22[0-3]|2[0-1][0-9]|[0-1][0-9][0-9]|"
                + "([0-9]){1,2})([.](25[0-5]|2[0-4][0-9]|[0-1][0-9][0-9]|"
                + "([0-9]){1,2})){3})|:))|(([0-9A-Fa-f]{1,4}:){3}((:[0-9A-Fa-f]{1,4}){1,4}|"
                + ":((22[0-3]|2[0-1][0-9]|[0-1][0-9][0-9]|"
                + "([0-9]){1,2})([.](25[0-5]|2[0-4][0-9]|"
                + "[0-1][0-9][0-9]|([0-9]){1,2})){3})|:))|"
                + "(([0-9A-Fa-f]{1,4}:){2}((:[0-9A-Fa-f]{1,4}){1,5}|"
                + ":((22[0-3]|2[0-1][0-9]|[0-1][0-9][0-9]|"
                + "([0-9]){1,2})([.](25[0-5]|2[0-4][0-9]|"
                + "[0-1][0-9][0-9]|([0-9]){1,2})){3})|:))"
                + "|(([0-9A-Fa-f]{1,4}:){1}((:[0-9A-Fa-f]{1,4}){1,6}"
                + "|:((22[0-3]|2[0-1][0-9]|[0-1][0-9][0-9]|"
                + "([0-9]){1,2})([.](25[0-5]|2[0-4][0-9]|"
                + "[0-1][0-9][0-9]|([0-9]){1,2})){3})|:))|"
                + "(:((:[0-9A-Fa-f]{1,4}){1,7}|(:[fF]{4}){0,1}:((22[0-3]|2[0-1][0-9]|"
                + "[0-1][0-9][0-9]|([0-9]){1,2})"
                + "([.](25[0-5]|2[0-4][0-9]|[0-1][0-9][0-9]|([0-9]){1,2})){3})|:)))$)";
        return hostName.matches(regex);
    }
}


