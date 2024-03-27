package com.xxdb.streaming.client.cep;

import java.util.List;

public class EventInfo {

    private List<AttributeSerializer> attributeSerializers;
    private EventSchemeEx eventScheme;

    public EventInfo(List<AttributeSerializer> attributeSerializers, EventSchemeEx eventScheme) {
        this.attributeSerializers = attributeSerializers;
        this.eventScheme = eventScheme;
    }

    public List<AttributeSerializer> getAttributeSerializers() {
        return this.attributeSerializers;
    }

    public EventSchemeEx getEventScheme() {
        return this.eventScheme;
    }
}
