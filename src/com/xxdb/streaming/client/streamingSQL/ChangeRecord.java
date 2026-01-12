package com.xxdb.streaming.client.streamingSQL;

import com.xxdb.data.Entity;

import java.util.Collections;
import java.util.List;

public class ChangeRecord {
    private final ChangeType type;
    private final int lineNo;
    private final List<Entity> rowData;

    public ChangeRecord(ChangeType type, int lineNo, List<Entity> rowData) {
        this.type = type == null ? ChangeType.UNKNOWN : type;
        this.lineNo = lineNo;
        this.rowData = rowData == null ? Collections.emptyList() : Collections.unmodifiableList(rowData);
    }

    public ChangeType getType() {
        return type;
    }

    public int getLineNo() {
        return lineNo;
    }

    public List<Entity> getRowData() {
        return rowData;
    }
}
