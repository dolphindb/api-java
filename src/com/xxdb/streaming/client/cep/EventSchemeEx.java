package com.xxdb.streaming.client.cep;

import java.util.ArrayList;
import java.util.List;

public class EventSchemeEx {

    private EventScheme scheme;
    private int timeIndex;
    private List<Integer> commonKeyIndex;

    public EventSchemeEx() {
        this.scheme = new EventScheme();
        this.commonKeyIndex = new ArrayList<>();
    }

    public EventScheme getScheme() {
        return this.scheme;
    }

    public void setScheme(EventScheme scheme) {
        this.scheme = scheme;
    }

    public int getTimeIndex() {
        return this.timeIndex;
    }

    public void setTimeIndex(int timeIndex) {
        this.timeIndex = timeIndex;
    }

    public List<Integer> getCommonKeyIndex() {
        return this.commonKeyIndex;
    }
}
