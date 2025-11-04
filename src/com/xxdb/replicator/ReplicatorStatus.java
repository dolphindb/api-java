package com.xxdb.replicator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Overall status information for the StreamReplicator.
 * Contains aggregated statistics and per-host status information.
 */
public class ReplicatorStatus {
    private long totalRows;
    private Map<String, ReplicatorStreamStatus> hostStatuses;

    /**
     * Creates a new ReplicatorStatus with default values.
     */
    public ReplicatorStatus() {
        this.totalRows = 0;
        this.hostStatuses = new HashMap<>();
    }

    /**
     * Creates a new ReplicatorStatus with the specified values.
     *
     * @param totalRows    The total number of rows inserted across all hosts
     * @param hostStatuses A map of host label to ReplicatorStreamStatus
     */
    public ReplicatorStatus(long totalRows, Map<String, ReplicatorStreamStatus> hostStatuses) {
        this.totalRows = totalRows;
        this.hostStatuses = hostStatuses == null ? new HashMap<>() : new HashMap<>(hostStatuses);
    }

    /**
     * Gets the total number of rows inserted across all hosts.
     *
     * @return The total number of rows
     */
    public long getTotalRows() {
        return totalRows;
    }

    /**
     * Sets the total number of rows inserted.
     *
     * @param totalRows The total number of rows
     */
    public void setTotalRows(long totalRows) {
        this.totalRows = totalRows;
    }

    /**
     * Gets an unmodifiable map of host labels to their status information.
     *
     * @return A map of host label to ReplicatorStreamStatus
     */
    public Map<String, ReplicatorStreamStatus> getHostStatuses() {
        return Collections.unmodifiableMap(hostStatuses);
    }

    /**
     * Sets the host statuses map.
     *
     * @param hostStatuses A map of host label to ReplicatorStreamStatus
     */
    public void setHostStatuses(Map<String, ReplicatorStreamStatus> hostStatuses) {
        this.hostStatuses = hostStatuses == null ? new HashMap<>() : new HashMap<>(hostStatuses);
    }

    /**
     * Gets the status for a specific host.
     *
     * @param label The host label
     * @return The ReplicatorStreamStatus for the host, or null if not found
     */
    public ReplicatorStreamStatus getHostStatus(String label) {
        return hostStatuses.get(label);
    }

    /**
     * Adds or updates the status for a specific host.
     *
     * @param label  The host label
     * @param status The status information
     */
    public void setHostStatus(String label, ReplicatorStreamStatus status) {
        if (label != null && status != null) {
            hostStatuses.put(label, status);
        }
    }

    /**
     * Gets the total number of dumped rows across all hosts.
     *
     * @return The sum of dumped rows from all hosts
     */
    public long getTotalDumpedRows() {
        return hostStatuses.values().stream()
            .mapToLong(ReplicatorStreamStatus::getDumpedRows)
            .sum();
    }

    /**
     * Gets the total number of pending rows across all hosts.
     *
     * @return The sum of pending rows from all hosts
     */
    public long getTotalPendingRows() {
        return hostStatuses.values().stream()
            .mapToLong(ReplicatorStreamStatus::getPendingRows)
            .sum();
    }

    /**
     * Checks if any host has an error.
     *
     * @return true if any host has an error, false otherwise
     */
    public boolean hasAnyError() {
        return hostStatuses.values().stream()
            .anyMatch(ReplicatorStreamStatus::hasError);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("ReplicatorStatus{totalRows=%d, totalDumped=%d, totalPending=%d%n",
            totalRows, getTotalDumpedRows(), getTotalPendingRows()));
        sb.append("  Host Statuses:");
        if (hostStatuses.isEmpty()) {
            sb.append(" (none)");
        } else {
            for (Map.Entry<String, ReplicatorStreamStatus> entry : hostStatuses.entrySet()) {
                sb.append(String.format("%n    %s: %s", entry.getKey(), entry.getValue()));
            }
        }
        sb.append("}");
        return sb.toString();
    }
}