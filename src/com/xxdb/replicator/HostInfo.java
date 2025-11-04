package com.xxdb.replicator;

/**
 * Host information for StreamReplicator connection.
 * Contains connection details for a DolphinDB server node.
 */
public class HostInfo {
    private final String host;
    private final int port;
    private final String userId;
    private final String password;
    private final String label;

    /**
     * Creates a new HostInfo with the specified connection details.
     *
     * @param host     The hostname or IP address of the DolphinDB server
     * @param port     The port number of the DolphinDB server
     * @param userId   The user ID for authentication
     * @param password The password for authentication
     * @param label    A unique label to identify this host
     */
    public HostInfo(String host, int port, String userId, String password, String label) {
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Host cannot be null or empty");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
        if (label == null || label.isEmpty()) {
            throw new IllegalArgumentException("Label cannot be null or empty");
        }

        this.host = host;
        this.port = port;
        this.userId = userId;
        this.password = password;
        this.label = label;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUserId() {
        return userId;
    }

    public String getPassword() {
        return password;
    }

    public String getLabel() {
        return label;
    }

    @Override
    public String toString() {
        return String.format("HostInfo{label='%s', host='%s', port=%d, userId='%s'}",
            label, host, port, userId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HostInfo hostInfo = (HostInfo) o;
        return port == hostInfo.port &&
                host.equals(hostInfo.host) &&
                label.equals(hostInfo.label);
    }

    @Override
    public int hashCode() {
        int result = host.hashCode();
        result = 31 * result + port;
        result = 31 * result + label.hashCode();
        return result;
    }
}