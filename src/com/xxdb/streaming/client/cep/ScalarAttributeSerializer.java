package com.xxdb.streaming.client.cep;

import com.xxdb.data.*;
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
        if (attribute.getDataType() == Entity.DATA_TYPE.DT_DECIMAL32)
            ((BasicDecimal32) attribute).writeScalarRawDataToOutputStream(out);
        else if (attribute.getDataType() == Entity.DATA_TYPE.DT_DECIMAL64) {
            ((BasicDecimal64) attribute).writeScalarRawDataToOutputStream(out);
        } else if (attribute.getDataType() == Entity.DATA_TYPE.DT_DECIMAL128) {
            ((BasicDecimal128) attribute).writeScalarRawDataToOutputStream(out);
        } else {
            ((AbstractScalar)attribute).writeScalarToOutputStream(out);
        }
    }

}
