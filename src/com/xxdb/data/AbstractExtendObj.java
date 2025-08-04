package com.xxdb.data;

import com.xxdb.io.ExtendedDataOutput;
import java.io.IOException;

public abstract class AbstractExtendObj extends AbstractEntity implements ExtendObj {

    @Override
    public DATA_FORM getDataForm() {
        return DATA_FORM.DF_EXTOBJ;
    }

    @Override
    public int rows() {
        return 1;
    }

    @Override
    public int columns() {
        return 1;
    }

    @Override
    public void write(ExtendedDataOutput output) throws IOException {
        throw new UnsupportedOperationException("Not support yet");
    }

    @Override
    public String toString(){
        return getString();
    }
}
