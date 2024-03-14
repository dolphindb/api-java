package com.xxdb.streaming.cep;

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
    private boolean isConnected;

    public EventSender(List<EventScheme> eventSchemes, List<String> eventTimeKeys, List<String> commonKeys) {
        this.eventHandler = new EventHandler(eventSchemes, eventTimeKeys, commonKeys);
        this.isConnected = false;
    }

    public void connect(String ip, int port, String user, String password, String tableName) throws IOException {
        if (this.isConnected)
            throw new RuntimeException("This eventSender is already connected");

        this.conn = new DBConnection();
        boolean success = this.conn.connect(ip, port, user, password, "", false, null,  true);
        if (!success) {
            throw new RuntimeException("connect dolphindb fail");
        }

        String sql = "select top 0 * from " + tableName;
        StringBuilder errMsg = new StringBuilder();
        BasicTable outputTable = (BasicTable) this.conn.run(sql);
        if (!this.eventHandler.checkOutputTable(outputTable, errMsg))
            throw new RuntimeException(errMsg.toString());

        this.insertScript = "tableInsert{" + tableName + "}";
        this.isConnected = true;
    }

    public void sendEvent(String eventType, List<Entity> attributes) {
        if (!isConnected)
            throw new RuntimeException("This eventSender has not connected to the dolphindb");

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

    public static EventSender createEventSender(List<EventScheme> eventSchemes, List<String> eventTimeKeys, List<String> commonKeys) {
        return new EventSender(eventSchemes, eventTimeKeys, commonKeys);
    }

}
