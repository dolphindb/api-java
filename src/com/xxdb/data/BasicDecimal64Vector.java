package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class BasicDecimal64Vector extends AbstractVector{
    private int scale_ = 0;
    private long[] values;
    private int size;
    private int capaticy;

    public BasicDecimal64Vector(int size){
        this(DATA_FORM.DF_VECTOR, size);
    }

    public BasicDecimal64Vector(DATA_FORM df, int size){
        super(df);
        values = new long[size];

        this.size = values.length;
        capaticy = values.length;
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
        byte[] buf = new byte[4096];
        while (off < totalBytes){
            int len = Math.min(4096, totalBytes - off);
            in.readFully(buf, 0, len);
            int start = off / 8, end = len / 8;
            ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
            for (int i = 0; i < end; i++){
                long value = byteBuffer.getLong(i * 8);
                values[i + start] = value;
            }
            off += len;
        }

        this.size = values.length;
        capaticy = values.length;
    }

    @Override
    protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {
        long[] data = new long[size];
        System.arraycopy(values, 0, data, 0, size);
        out.writeInt(scale_);
        out.writeLongArray(data);
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
    public Entity get(int index) {
        return new BasicDecimal64(scale_, values[index]);
    }

    @Override
    public void set(int index, Entity value) throws Exception {
        if (((Scalar)value).getScale() != scale_)
            throw new RuntimeException("Value's scale is not the same as the vector's!");
        else
            scale_ = ((Scalar) value).getScale();
        if(((Scalar)value).isNull()){
            values[index] = Integer.MIN_VALUE;
        }else{
            values[index] = ((Scalar)value).getNumber().longValue();
        }
    }

    @Override
    public Class<?> getElementClass() {
        return BasicDecimal64.class;
    }

    @Override
    public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {
        throw new RuntimeException("Decimal32 does not support arrayVector");
    }

    @Override
    public int getUnitLength() {
        return 8;
    }


    public void add(long value) {
        if (size + 1 > capaticy && values.length > 0){
            values = Arrays.copyOf(values, values.length * 2);
        }else if (values.length <= 0){
            values = Arrays.copyOf(values, values.length + 1);
        }
        capaticy = values.length;
        values[size] = value;
        size++;
    }


    public void addRange(long[] valueList) {
        values = Arrays.copyOf(values, valueList.length + values.length);
        System.arraycopy(valueList, 0, values, size, valueList.length);
        size += valueList.length;
        capaticy = values.length;
    }

    @Override
    public void Append(Scalar value) throws Exception{
        add(value.getNumber().longValue());
    }

    @Override
    public void Append(Vector value) throws Exception{
        addRange(((BasicDecimal64Vector)value).getdataArray());
    }

    public long[] getdataArray(){
        long[] data = new long[size];
        System.arraycopy(values, 0, data, 0, size);
        return data;
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
        return size;
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
            out.putLong(values[indexStart + i]);
        }
        numElementAndPartial.numElement = targetNumElement;
        numElementAndPartial.partial = 0;
        return targetNumElement * 8;
    }
}
