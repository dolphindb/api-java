package com.xxdb.compression;

import com.xxdb.data.BasicTable;
import com.xxdb.data.Vector;
import com.xxdb.io.BigEndianDataOutputStream;
import com.xxdb.io.ExtendedDataOutput;
import com.xxdb.io.LittleEndianDataOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BasicTableCompressor {

    public static void compressBasicTable(BasicTable table, int method, ExtendedDataOutput output, boolean isLittleEndian) throws Exception {
        //FIXME: create header
        int rows = table.rows();
        int cols = table.columns();
        output.writeInt(rows);
        output.writeInt(cols);
        output.writeString("pt");
        for (int i = 0; i < cols; i++) {
            output.writeString(table.getColumnName(i));
        }
        output.writeShort(0);
        for (int i = 0; i < cols; i++) {
            ByteBuffer compressedCol = compressVector(table.getColumn(i), rows * Long.BYTES, isLittleEndian, method);
            output.write(compressedCol.array(), 0, compressedCol.position() + 1);
        }
    }

    private static ByteBuffer compressVector(Vector v, int maxCompressedLength, boolean isLittleEndian, int method) throws Exception {
        int elementCount = v.rows();
        int dataType = v.getDataType().ordinal();
        int unitLength;
        if (dataType >= 6 && dataType <= 11 || dataType == 4) {
            unitLength = 4;
        } else if (dataType >= 12 && dataType <= 14 || dataType == 5 || dataType == 16) {
            unitLength = 8;
        } else if (dataType == 3) {
            unitLength = 2;
        } else {
            throw new RuntimeException("Compression Failed: only support integral and temporal data");
        }
        ByteBuffer out = isLittleEndian ?
                ByteBuffer.allocate(elementCount * unitLength + 50).order(ByteOrder.LITTLE_ENDIAN) :
                ByteBuffer.allocate(elementCount * unitLength + 50).order(ByteOrder.BIG_ENDIAN);
        out.putInt(0);//compressedBytes
        out.position(out.position() + 7);
        out.put((byte) method);
        out.put((byte) dataType);
        out.put((byte) unitLength);
        out.position(out.position() + 6);
        out.putInt(elementCount);
        out.putInt(0); //TODO: checkSum
        ByteBuffer in = ByteBuffer.allocate(elementCount * unitLength);
        readVector(v, in, unitLength);
        int compressedLength = EncoderFactory.get(method).compress(in, elementCount, unitLength, maxCompressedLength, out);
        out.putInt(0, compressedLength);
        return out;
    }

    private static void readVector(Vector v, ByteBuffer buffer, int unitLength) throws Exception {
        for (int i = 0; i < v.rows(); i++) {
            if (unitLength == 2)
                buffer.putShort(v.get(i).getNumber().shortValue());
            else if (unitLength == 4)
                buffer.putInt(v.get(i).getNumber().intValue());
            else if (unitLength == 8)
                buffer.putLong(v.get(i).getNumber().longValue());
        }
        buffer.flip();
    }
}
