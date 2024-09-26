package com.xxdb.streaming.client.cep;

import java.util.ArrayList;
import java.util.List;

public class EventSchemaEx {

    private EventSchema schema;
    private int timeIndex;
    private List<Integer> commonFieldIndex;

    public EventSchemaEx() {
        this.schema = new EventSchema();
        this.commonFieldIndex = new ArrayList<>();
    }

    public EventSchema getSchema() {
        return this.schema;
    }

    public void setSchema(EventSchema schema) {
        this.schema = schema;
    }

    public int getTimeIndex() {
        return this.timeIndex;
    }

    public void setTimeIndex(int timeIndex) {
        this.timeIndex = timeIndex;
    }

    public List<Integer> getCommonFieldIndex() {
        return this.commonFieldIndex;
    }
}
