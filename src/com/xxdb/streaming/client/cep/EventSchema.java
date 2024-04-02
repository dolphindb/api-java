package com.xxdb.streaming.client.cep;

import com.xxdb.data.Entity;
import java.util.ArrayList;
import java.util.List;

public class EventSchema {

    private String eventType;
    private List<String> fieldNames;
    private List<Entity.DATA_TYPE> fieldTypes;
    private List<Entity.DATA_FORM> fieldForms;
    private List<Integer> fieldExtraParams;

    public EventSchema() {
        this.fieldNames = new ArrayList<>();
        this.fieldTypes = new ArrayList<>();
        this.fieldForms = new ArrayList<>();
        this.fieldExtraParams = new ArrayList<>();
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public String getEventType() {
        return this.eventType;
    }

    public List<Entity.DATA_TYPE> getFieldTypes() {
        return this.fieldTypes;
    }

    public List<Entity.DATA_FORM> getFieldForms() {
        return this.fieldForms;
    }

    public List<Integer> getFieldExtraParams() {
        return this.fieldExtraParams;
    }

    public void setFieldExtraParams(List<Integer> fieldExtraParams) {
        this.fieldExtraParams = fieldExtraParams;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public void setFieldNames(List<String> fieldNames) {
        this.fieldNames = fieldNames;
    }

    public void setFieldTypes(List<Entity.DATA_TYPE> fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

    public void setFieldForms(List<Entity.DATA_FORM> fieldForms) {
        this.fieldForms = fieldForms;
    }
}
