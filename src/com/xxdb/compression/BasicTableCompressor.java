package com.xxdb.compression;

import com.xxdb.data.BasicTable;
import com.xxdb.data.Entity;
import com.xxdb.data.Vector;
import com.xxdb.io.ExtendedDataOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BasicTableCompressor {

    public static void compressBasicTable(BasicTable table, int method, ExtendedDataOutput output, boolean isLittleEndian) throws IOException {
        short flag = (short) (Entity.DATA_FORM.DF_TABLE.ordinal() << 8 | 8 & 0xff); //8: table type
        output.writeShort(flag); //need to delete in local test

        int rows = table.rows();
        int cols = table.columns();
        output.writeInt(rows);
        output.writeInt(cols);
        output.writeString(""); //table name
        for (int i = 0; i < cols; i++) {
            output.writeString(table.getColumnName(i));
        }

        for (int i = 0; i < cols; i++) {
            ByteBuffer compressedCol = compressVector(table.getColumn(i), rows * Long.BYTES * 2, isLittleEndian, method);
            output.write(compressedCol.array(), 0, compressedCol.position());
        }
    }

    private static ByteBuffer compressVector(Vector v, int maxCompressedLength, boolean isLittleEndian, int method) throws IOException {
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
        int elementCount = v.rows();
        ByteBuffer out = isLittleEndian ?
                ByteBuffer.allocate(maxCompressedLength).order(ByteOrder.LITTLE_ENDIAN) :
                ByteBuffer.allocate(maxCompressedLength).order(ByteOrder.BIG_ENDIAN);
        short flag = (short) (Entity.DATA_FORM.DF_VECTOR.ordinal() << 8 | Entity.DATA_TYPE.DT_COMPRESS.ordinal() & 0xff);

        out.putShort(flag);
        out.putInt(0);// compressedBytes
        out.putInt(1);// cols
        out.put((byte) 0); // version
        out.put((byte) 1); // flag bit0:littleEndian bit1:containChecksum
        out.put((byte) -1); // charcode
        out.put((byte) method);
        out.put((byte) dataType);
        out.put((byte) unitLength);
        out.position(out.position() + 2); //reserved
        out.putInt(-1); //extra
        out.putInt(elementCount);
        out.putInt(-1); //TODO: checkSum

        ByteBuffer in = ByteBuffer.allocate(elementCount * unitLength);
        readVector(v, in, unitLength);
        int compressedLength = EncoderFactory.get(method).compress(in, elementCount, unitLength, maxCompressedLength, out);
        out.putInt(2, compressedLength + 20);
        return out;
    }

    private static void readVector(Vector v, ByteBuffer buffer, int unitLength) throws IOException {
        try {
            for (int i = 0; i < v.rows(); i++) {
                if (unitLength == 2)
                    buffer.putShort(v.get(i).getNumber().shortValue());
                else if (unitLength == 4)
                    buffer.putInt(v.get(i).getNumber().intValue());
                else if (unitLength == 8)
                    buffer.putLong(v.get(i).getNumber().longValue());
            }
            buffer.flip();
        } catch (Exception ex) {
            throw new IOException("Invalid datatype");
        }
    }
}
