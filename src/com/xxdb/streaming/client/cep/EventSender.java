package com.xxdb.streaming.client.cep;

import com.xxdb.DBConnection;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Entity;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EventSender {

    private String insertScript;
    private EventHandler eventHandler;
    private DBConnection conn;

    public EventSender(DBConnection conn, String tableName, List<EventSchema> eventSchemas, List<String> eventTimeFields, List<String> commonFields) throws IOException {
        this.eventHandler = new EventHandler(eventSchemas, eventTimeFields, commonFields);
        this.conn = conn;

        String sql = "select top 0 * from " + tableName;
        StringBuilder errMsg = new StringBuilder();
        BasicTable inputTable = (BasicTable) this.conn.run(sql);
        if (!this.eventHandler.checkInputTable(tableName, inputTable, errMsg))
            throw new RuntimeException(errMsg.toString());

        this.insertScript = "tableInsert{" + tableName + "}";
    }

    public void sendEvent(String eventType, List<Entity> attributes) {
        List<Entity> args = new ArrayList<>();
        StringBuilder errMsg = new StringBuilder();

        if (!eventHandler.serializeEvent(eventType, attributes, args, errMsg))
            throw new RuntimeException("serialize event Fail for " + errMsg);

        try {
            conn.run(insertScript, args);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
