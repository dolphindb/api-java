package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BasicDecimal64Vector extends AbstractVector{
    protected int scale_ = 0;
    protected long[] values;

    public BasicDecimal64Vector(DATA_FORM df) {
        super(df);
    }

    public BasicDecimal64Vector(int size){
        this(DATA_FORM.DF_VECTOR, size);
    }

    public BasicDecimal64Vector(DATA_FORM df, int size){
        super(df);
        values = new long[size];
    }

    public BasicDecimal64Vector(DATA_FORM df, ExtendedDataInput in) throws IOException{
        super(df);
        int rows = in.readInt();
        int cols = in.readInt();
        int size = rows * cols;
        values = new long[size];
        scale_ = in.readInt();
        int totalBytes = size * 8;
        int off = 0;
        boolean little = in.isLittleEndian();
        ByteOrder bo = little ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
        while (off < totalBytes){
            int len = Math.min(BUF_SIZE, totalBytes - off);
            in.readFully(buf, 0, len);
            int start = off / 8, end = len / 8;
            ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
            for (int i = 0; i < end; i++){
                long value = byteBuffer.getLong(i * 8);
                values[i + start] = value;
            }
            off += len;
        }
    }

    @Override
    protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {

    }

    @Override
    public Vector combine(Vector vector) {
        return null;
    }

    @Override
    public Vector getSubVector(int[] indices) {
        return null;
    }

    @Override
    public int asof(Scalar value) {
        return 0;
    }

    @Override
    public boolean isNull(int index) {
        return values[index] == Long.MIN_VALUE;
    }

    @Override
    public void setNull(int index) {
        values[index] = Long.MIN_VALUE;
    }

    @Override
    public Scalar get(int index) {
        return new BasicDecimal64(scale_, values[index]);
    }

    @Override
    public void set(int index, Scalar value) throws Exception {

    }

    @Override
    public Class<?> getElementClass() {
        return BasicDecimal64.class;
    }

    @Override
    public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {

    }

    @Override
    public int getUnitLength() {
        return 8;
    }

    @Override
    public DATA_CATEGORY getDataCategory() {
        return DATA_CATEGORY.DECIMAL;
    }

    @Override
    public DATA_TYPE getDataType() {
        return DATA_TYPE.DT_DECIMAL64;
    }

    @Override
    public int rows() {
        return values.length;
    }
}
