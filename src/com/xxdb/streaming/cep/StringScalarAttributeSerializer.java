package com.xxdb.streaming.cep;

import com.xxdb.data.Entity;
import com.xxdb.io.ExtendedDataOutput;
import java.io.IOException;

public class StringScalarAttributeSerializer extends AttributeSerializer {

    private boolean isBlob;

    public StringScalarAttributeSerializer(boolean isBlob) {
        super(-1, Entity.DATA_FORM.DF_SCALAR);
        this.isBlob = isBlob;
    }

    @Override
    public void serialize(Entity attribute, ExtendedDataOutput outStream) throws IOException {
        // Implementation omitted
    }

}
