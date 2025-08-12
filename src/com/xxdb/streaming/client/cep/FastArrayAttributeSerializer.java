package com.xxdb.streaming.client.cep;

import com.xxdb.data.AbstractVector;
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
        if (curCount == 0) {
            out.writeShort((short)0);
            return;
        }

        // Using signed types, but ensuring they are processed according to unsigned logic
        short arrayRows = 1;
        byte curCountBytes = 1;
        byte reserved = 0;
        int maxCount = 255;

        while (curCount > maxCount) {
            curCountBytes *= 2;
            maxCount = (int)((1L << (8 * curCountBytes)) - 1);
        }

        out.writeShort(arrayRows);
        out.writeByte(curCountBytes);
        out.writeByte(reserved);

        switch (curCountBytes) {
            case 1:
            {
                // byte range: -128 to 127, but treated as unsigned for range 0 to 255
                out.writeByte((byte)(curCount & 0xFF));
                break;
            }
            case 2:
            {
                // short range: -32768 to 32767, but treated as unsigned for range 0 to 65535
                out.writeShort((short)(curCount & 0xFFFF));
                break;
            }
            default:
            {
                // int range: -2147483648 to 2147483647, sufficient for most cases
                out.writeInt(curCount);
                break;
            }
        }

        ((AbstractVector)attribute).serialize(0, curCount, out);
    }
}
