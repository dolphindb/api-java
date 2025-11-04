package com.xxdb.comm;

/**
 * Represents the connection state of a replication target.
 */
public enum ConnectionState {
    /**
     * The connection is being initialized.
     */
    Initializing,

    /**
     * The connection is active and ready for data transfer.
     */
    Connected,

    /**
     * The connection has been terminated.
     */
    Terminated,

    /**
     * The connection is attempting to reconnect.
     */
    Reconnecting
}