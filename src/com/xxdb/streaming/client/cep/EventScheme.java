package com.xxdb.streaming.client.cep;

import com.xxdb.data.Entity;
import java.util.ArrayList;
import java.util.List;

public class EventScheme {

    private String eventType;
    private List<String> attrKeys;
    private List<Entity.DATA_TYPE> attrTypes;
    private List<Entity.DATA_FORM> attrForms;
    private List<Integer> attrExtraParams;

    public EventScheme() {
        this.attrKeys = new ArrayList<>();
        this.attrTypes = new ArrayList<>();
        this.attrForms = new ArrayList<>();
        this.attrExtraParams = new ArrayList<>();
    }

    public List<String> getAttrKeys() {
        return attrKeys;
    }

    public String getEventType() {
        return this.eventType;
    }

    public List<Entity.DATA_TYPE> getAttrTypes() {
        return this.attrTypes;
    }

    public List<Entity.DATA_FORM> getAttrForms() {
        return this.attrForms;
    }

    public List<Integer> getAttrExtraParams() {
        return this.attrExtraParams;
    }

    public void setAttrExtraParams(List<Integer> attrExtraParams) {
        this.attrExtraParams = attrExtraParams;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public void setAttrKeys(List<String> attrKeys) {
        this.attrKeys = attrKeys;
    }

    public void setAttrTypes(List<Entity.DATA_TYPE> attrTypes) {
        this.attrTypes = attrTypes;
    }

    public void setAttrForms(List<Entity.DATA_FORM> attrForms) {
        this.attrForms = attrForms;
    }
}