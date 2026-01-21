package com.xxdb.streaming.client.cep;

import com.xxdb.DBConnection;
import com.xxdb.data.BasicDictionary;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventSender {

    private String insertScript;
    private EventHandler eventHandler;
    private DBConnection conn;

    private static final Logger log = LoggerFactory.getLogger(EventSender.class);

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
            throw new RuntimeException("serialize event fail for " + errMsg);

        try {
            conn.run(insertScript, args);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Entity appendEventWithResponse(String engine, String eventType, List<Entity> attributes, String responseType, int timeout, String condition) {
        if (engine == null || engine.isEmpty())
            throw new IllegalArgumentException("engine cannot be null or empty.");
        if (eventType == null || eventType.isEmpty())
            throw new IllegalArgumentException("eventType cannot be null or empty.");
        if (responseType == null || responseType.isEmpty())
            throw new IllegalArgumentException("responseType cannot be null or empty.");
        if (attributes == null)
            throw new IllegalArgumentException("attributes cannot be null.");
        if (timeout <= 0) {
            log.warn("The param 'timeout' cannot be less than or equal to 0, the default value of 5000 will be used.");
            timeout = 5000;
        }

        StringBuilder errMsg = new StringBuilder();
        BasicDictionary event = eventHandler.toEventDictionary(eventType, attributes, errMsg);
        if (event == null)
            throw new RuntimeException("build event fail for " + errMsg);

        try {
            String eventVar ="cepEvent" + "_" + System.nanoTime();
            Map<String, Entity> uploadMap = new HashMap<>();
            uploadMap.put(eventVar, event);

            conn.upload(uploadMap);

            String responseVar = "cepResp" + "_" + System.nanoTime();
            String script = buildAppendEventScript(responseVar, engine, responseType, timeout, eventVar, condition);
            return conn.run(script);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String buildAppendEventScript(String responseVar, String engine, String responseType, int timeout,
                                                 String eventVar, String condition) {
        StringBuilder script = new StringBuilder();
        script.append(responseVar)
                .append(" = appendEventWithResponse(engine=\"")
                .append(engine)
                .append("\", event=")
                .append(eventVar)
                .append(", responseType=\"")
                .append(responseType)
                .append("\", timeout=")
                .append(timeout);
        if (condition != null && !condition.isEmpty()) {
            String trimmed = condition.trim();
            if (trimmed.startsWith("<") && trimmed.endsWith(">")) {
                script.append(", condition=").append(trimmed);
            } else {
                script.append(", condition=<").append(condition).append(">");
            }
        }
        script.append(", returnType=\"dict\");\n");
        script.append("try{undef(`").append(eventVar).append(")}catch(ex){}\n");
        script.append(responseVar);
        return script.toString();
    }
}
