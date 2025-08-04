package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;
import java.io.IOException;

public class BasicInstrument extends AbstractExtendObj implements Comparable<BasicInstrument> {

    public BasicInstrument(ExtendedDataInput in) throws IOException {
        int extendType = in.readInt();
        int versionAndSize = in.readInt();
        int version = versionAndSize >> 24;
        int size = versionAndSize & 0xFFFFFF;
        for (int i = 0; i < size; i++ ) {
            in.readByte();
        }
    }

    @Override
    public DATA_CATEGORY getDataCategory() {
        return DATA_CATEGORY.SYSTEM;
    }

    @Override
    public DATA_TYPE getDataType() {
        return DATA_TYPE.DT_INSTRUMENT;
    }

    @Override
    public String getString() {
        return "Instrument<detail invisible>";
    }

    @Override
    public int compareTo(BasicInstrument o) {
        throw new RuntimeException("BasicInstrument.compareTo not supported.");
    }
}
