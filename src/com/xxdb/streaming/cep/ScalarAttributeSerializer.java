package com.xxdb.streaming.cep;

import com.xxdb.data.AbstractScalar;
import com.xxdb.data.Entity;
import com.xxdb.io.ExtendedDataOutput;
import java.io.IOException;

public class ScalarAttributeSerializer  extends AttributeSerializer {

    private String buf;

    public ScalarAttributeSerializer(int unitLen) {
        super(unitLen, Entity.DATA_FORM.DF_SCALAR);
        this.buf = ""; // Initialize with empty string
    }

    @Override
    public void serialize(Entity attribute, ExtendedDataOutput out) throws IOException {
        ((AbstractScalar)attribute).writeScalarToOutputStream(out);
    }

}
