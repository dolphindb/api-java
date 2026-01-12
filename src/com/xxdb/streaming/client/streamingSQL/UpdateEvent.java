package com.xxdb.streaming.client.streamingSQL;

import com.xxdb.data.BasicTable;
import com.xxdb.streaming.client.IMessage;

import java.util.Collections;
import java.util.List;

public class UpdateEvent {
    private final String queryId;
    private final BasicTable table;
    private final List<ChangeRecord> changeRecords;
    private final List<IMessage> rawMessages;
    private final long appliedAtMillis;

    public UpdateEvent(String queryId, BasicTable table, List<ChangeRecord> changeRecords, List<IMessage> rawMessages, long appliedAtMillis) {
        this.queryId = queryId;
        this.table = table;
        this.changeRecords = changeRecords == null ? Collections.emptyList() : Collections.unmodifiableList(changeRecords);
        this.rawMessages = rawMessages == null ? Collections.emptyList() : Collections.unmodifiableList(rawMessages);
        this.appliedAtMillis = appliedAtMillis;
    }

    public String getQueryId() {
        return queryId;
    }

    public BasicTable getTable() {
        return table;
    }

    public List<ChangeRecord> getChangeRecords() {
        return changeRecords;
    }

    public List<IMessage> getRawMessages() {
        return rawMessages;
    }

    public long getAppliedAtMillis() {
        return appliedAtMillis;
    }
}
