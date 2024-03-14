package com.xxdb.streaming.cep;

import com.xxdb.data.Entity;
import com.xxdb.io.ExtendedDataOutput;
import java.io.IOException;

public class FastArrayAttributeSerializer extends AttributeSerializer {

    public FastArrayAttributeSerializer(int unitLen) {
        super(unitLen, Entity.DATA_FORM.DF_VECTOR);
    }

    @Override
    public void serialize(Entity attribute, ExtendedDataOutput out) throws IOException {
        int curCount = attribute.rows();
        short arrayRows = 1;
        byte curCountBytes = 1;
        byte reserved = 0;
        int maxCount = 255;
        while (curCount > maxCount) {
            curCountBytes *= 2;
            maxCount = (1 << (8 * curCountBytes)) - 1;
        }
        out.writeShort(arrayRows);
        out.writeByte(curCountBytes);
        out.writeByte(reserved);
        switch (curCountBytes) {
            case 1:
                out.writeByte(curCount);
                break;
            case 2:
                out.writeShort(curCount);
                break;
            default:
                out.writeInt(curCount);
                break;
        }

        attribute.write(out);
    }

}
