package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BasicDecimal32Vector extends AbstractVector{
    protected int scale_ = 0;
    protected int[] values;

    public BasicDecimal32Vector(DATA_FORM df) {
        super(df);
    }

    public BasicDecimal32Vector(int size){
        this(DATA_FORM.DF_VECTOR, size);
    }

    public BasicDecimal32Vector(DATA_FORM df, int size){
        super(df);
        values = new int[size];
    }

    public BasicDecimal32Vector(DATA_FORM df, ExtendedDataInput in) throws IOException{
        super(df);
        int rows = in.readInt();
        int cols = in.readInt();
        int size = rows * cols;
        values = new int[size];
        scale_ = in.readInt();
        int totalBytes = size * 4;
        int off = 0;
        boolean little = in.isLittleEndian();
        ByteOrder bo = little ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
        while (off < totalBytes){
            int len = Math.min(BUF_SIZE, totalBytes - off);
            in.readFully(buf, 0, len);
            int start = off / 4, end = len / 4;
            ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
            for (int i = 0; i < end; i++){
                int value = byteBuffer.getInt(i * 4);
                values[i + start] = value;
            }
            off += len;
        }
    }

    @Override
    protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {
        out.writeInt(scale_);
        out.writeIntArray(values);
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
        return values[index] == Integer.MIN_VALUE;
    }

    @Override
    public void setNull(int index) {
        values[index] = Integer.MIN_VALUE;
    }

    @Override
    public Scalar get(int index) {
        return new BasicDecimal32(new int[]{scale_, values[index]});
    }

    @Override
    public void set(int index, Scalar value) throws Exception {

    }

    @Override
    public Class<?> getElementClass() {
        return BasicDecimal32.class;
    }

    @Override
    public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {

    }

    @Override
    public int getUnitLength() {
        return 4;
    }

    @Override
    public DATA_CATEGORY getDataCategory() {
        return DATA_CATEGORY.DECIMAL;
    }

    @Override
    public DATA_TYPE getDataType() {
        return DATA_TYPE.DT_DECIMAL32;
    }

    @Override
    public int rows() {
        return values.length;
    }

    @Override
    public int getExtraParamForType(){
        return scale_;
    }

    @Override
    public int serialize(int indexStart, int offect, int targetNumElement, NumElementAndPartial numElementAndPartial, ByteBuffer out) throws IOException{
        targetNumElement = Math.min((out.remaining() / getUnitLength()), targetNumElement);
        for (int i = 0; i < targetNumElement; ++i)
        {
            out.putInt(values[indexStart + i]);
        }
        numElementAndPartial.numElement = targetNumElement;
        numElementAndPartial.partial = 0;
        return targetNumElement * 4;
    }
}
