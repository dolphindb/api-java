package com.xxdb.streaming.client.cep;

import com.xxdb.data.Entity;
import com.xxdb.io.ExtendedDataOutput;
import java.io.IOException;

public class FastArrayAttributeSerializer extends AttributeSerializer {

    public FastArrayAttributeSerializer(int unitLen) {
        super(unitLen, Entity.DATA_FORM.DF_VECTOR);
    }

    @Override
    public void serialize(Entity attribute, ExtendedDataOutput out) throws IOException {
        attribute.write(out);
    }

}
