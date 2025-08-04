package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;
import java.io.IOException;

public class BasicMktData extends AbstractExtendObj implements Comparable<BasicMktData> {

    public BasicMktData(ExtendedDataInput in) throws IOException {
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
        return DATA_TYPE.DT_MKTDATA;
    }

    @Override
    public String getString() {
        return "MktData<detail invisible>";
    }

    @Override
    public int compareTo(BasicMktData o) {
        throw new RuntimeException("BasicMktData.compareTo not supported.");
    }
}
