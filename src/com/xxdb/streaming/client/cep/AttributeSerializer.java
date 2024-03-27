package com.xxdb.streaming.client.cep;

import com.xxdb.data.Entity;
import com.xxdb.io.ExtendedDataOutput;
import java.io.IOException;

public class AttributeSerializer {

    protected int unitLen;
    protected Entity.DATA_FORM form;

    public AttributeSerializer(int unitLen, Entity.DATA_FORM form) {
        this.unitLen = unitLen;
        this.form = form;
    }

    public void serialize(Entity attribute, ExtendedDataOutput out) throws IOException {
        if(this.form == Entity.DATA_FORM.DF_SCALAR || attribute.getDataForm() == this.form)
            attribute.write(out);
        else
            throw new RuntimeException("Invalid data.");
    }

    public Entity.DATA_FORM getForm() {
        return this.form;
    }

}
