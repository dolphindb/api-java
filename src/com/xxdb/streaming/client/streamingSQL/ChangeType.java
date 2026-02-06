package com.xxdb.streaming.client.streamingSQL;

public enum ChangeType {
    UPDATE,
    APPEND,
    DELETE,
    INSERT,
    UNKNOWN;

    public static ChangeType toChangeType(byte type) {
        switch (type) {
            case 0:
                return ChangeType.UPDATE;
            case 1:
                return ChangeType.APPEND;
            case 2:
                return ChangeType.DELETE;
            case 3:
                return ChangeType.INSERT;
            default:
                return ChangeType.UNKNOWN;
        }
    }
}