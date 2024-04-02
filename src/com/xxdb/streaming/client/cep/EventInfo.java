package com.xxdb.streaming.client.cep;

import java.util.List;

public class EventInfo {

    private List<AttributeSerializer> attributeSerializers;
    private EventSchemaEx eventSchema;

    public EventInfo(List<AttributeSerializer> attributeSerializers, EventSchemaEx eventSchema) {
        this.attributeSerializers = attributeSerializers;
        this.eventSchema = eventSchema;
    }

    public List<AttributeSerializer> getAttributeSerializers() {
        return this.attributeSerializers;
    }

    public EventSchemaEx getEventSchema() {
        return this.eventSchema;
    }
}
