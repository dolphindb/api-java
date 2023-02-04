package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class BasicDecimal64Vector extends AbstractVector{
    private int scale_ = -1;
    private long[] values;
    private int size;
    private int capaticy;

    BasicDecimal64Vector(int size){
        this(DATA_FORM.DF_VECTOR, size);
    }

    public BasicDecimal64Vector(int size, int scale){
        super(DATA_FORM.DF_VECTOR);
        this.scale_ = scale;
        values = new long[size];

        this.size = values.length;
        capaticy = values.length;
    }

    BasicDecimal64Vector(DATA_FORM df, int size){
        super(df);
        values = new long[size];

        this.size = values.length;
        capaticy = values.length;
    }

    BasicDecimal64Vector(long[] dataValue, int scale){
        super(DATA_FORM.DF_VECTOR);
        this.scale_ = scale;
        this.values = dataValue;
        this.size = values.length;
        capaticy = values.length;
    }

    public BasicDecimal64Vector(DATA_FORM df, ExtendedDataInput in, int extra) throws IOException{
        super(df);
        int rows = in.readInt();
        int cols = in.readInt();
        int size = rows * cols;
        values = new long[size];
        if (extra != -1)
            scale_ = extra;
        else
            scale_ = in.readInt();
        long totalBytes = (long)size * 8;
        long off = 0;
        boolean little = in.isLittleEndian();
        ByteOrder bo = little ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
        byte[] buf = new byte[4096];
        while (off < totalBytes){
            int len = (int)Math.min(4096, totalBytes - off);
            in.readFully(buf, 0, len);
            int start = (int)(off / 8), end = len / 8;
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

    public BasicDecimal64Vector(double[] data, int scale){
        super(DATA_FORM.DF_VECTOR);
        if (scale < 0 || scale > 18)
            throw new RuntimeException("Scale out of bound (valid range: [0, 9], but get: " + scale + ")");
        scale_ = scale;
        long[] newIntValue = new long[data.length];
        for(int i = 0; i < data.length; i++){
            BigDecimal pow = new BigDecimal(1);
            for (long j = 0; j < scale_; j++) {
                pow = pow.multiply(new BigDecimal(10));
            }
            BigDecimal dbvalue = new BigDecimal(Double.toString(data[i]));
            newIntValue[i] = (dbvalue.multiply(pow)).longValue();
        }
        values = newIntValue;
        this.size = values.length;
        capaticy = values.length;
    }

    @Override
    public void deserialize(int start, int count, ExtendedDataInput in) throws IOException{
        long totalBytes = (long)count * 8, off = 0;
        ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
        byte[] buf = new byte[4096];
        while (off < totalBytes) {
            int len = (int)Math.min(4096, totalBytes - off);
            in.readFully(buf, 0, len);
            int end = len / 8;
            ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
            for (int i = 0; i < end; i++)
                values[i + start] = byteBuffer.getLong(i * 8);
            off += len;
            start += end;
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
    public void setExtraParamForType(int scale){
        this.scale_ = scale;
    }

    @Override
    public Vector combine(Vector vector) {
        return null;
    }

    @Override
    public Vector getSubVector(int[] indices) {
        int length = indices.length;
        long[] sub = new long[length];
        for(int i=0; i<length; ++i)
            sub[i] = values[indices[i]];
        return new BasicDecimal64Vector(sub, scale_);
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
        if (scale_ < 0)
            throw new RuntimeException("Please set scale first.");
        if(((Scalar)value).isNull())
            values[index] = Long.MIN_VALUE;
        else{
            double data = ((Scalar)value).getNumber().doubleValue();
            if (data == 0.0)
                values[index] = 0;
            else {
                BigDecimal pow = new BigDecimal(1);
                for (long i = 0; i < scale_; i++) {
                    pow = pow.multiply(new BigDecimal(10));
                }
                BigDecimal dbvalue = new BigDecimal(Double.toString(data));
                values[index] = (dbvalue.multiply(pow)).longValue();
            }
        }
    }

    @Override
    public Class<?> getElementClass() {
        return BasicDecimal64.class;
    }

    @Override
    public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {
        for (int i = 0; i < count; i++){
            out.writeLong(values[start + i]);
        }
    }

    @Override
    public int getUnitLength() {
        return 8;
    }


    public void add(double value) {
        if (scale_ < 0){
            throw new RuntimeException("Please set scale first.");
        }
        if (size + 1 > capaticy && values.length > 0){
            values = Arrays.copyOf(values, values.length * 2);
        }else if (values.length <= 0){
            values = Arrays.copyOf(values, values.length + 1);
        }
        capaticy = values.length;
        if (value == 0.0)
            values[size] = 0;
        else {
            BigDecimal pow = new BigDecimal(1);
            for (long i = 0; i < scale_; i++) {
                pow = pow.multiply(new BigDecimal(10));
            }
            BigDecimal dbvalue = new BigDecimal(Double.toString(value));
            values[size] = (dbvalue.multiply(pow)).longValue();
        }
        size++;
    }


    void addRange(long[] valueList) {
        values = Arrays.copyOf(values, valueList.length + values.length);
        System.arraycopy(valueList, 0, values, size, valueList.length);
        size += valueList.length;
        capaticy = values.length;
    }

    public void addRange(double[] valueList) {
        if (scale_ < 0){
            throw new RuntimeException("Please set scale first.");
        }
        long[] newLongValue = new long[valueList.length];
        for(int i = 0; i < valueList.length; i++){
            BigDecimal pow = new BigDecimal(1);
            for (long j = 0; j < scale_; j++) {
                pow = pow.multiply(new BigDecimal(10));
            }
            BigDecimal dbvalue = new BigDecimal(Double.toString(valueList[i]));
            newLongValue[i] = (dbvalue.multiply(pow)).longValue();
        }
        values = Arrays.copyOf(values, newLongValue.length + values.length);
        System.arraycopy(newLongValue, 0, values, size, newLongValue.length);
        size += newLongValue.length;
        capaticy = values.length;
    }

    @Override
    public void Append(Scalar value) throws Exception{
        if (scale_ < 0)
            throw new RuntimeException("Please set scale first.");
        add(value.getNumber().doubleValue());
    }

    @Override
    public void Append(Vector value) throws Exception{
        if (scale_ < 0)
            throw new RuntimeException("Please set scale first.");
        addRange(((BasicDecimal64Vector)value).getdataArray());
    }

    public long[] getdataArray(){
        long[] data = new long[size];
        System.arraycopy(values, 0, data, 0, size);
        return data;
    }

    public void setScale(int scale){
        this.scale_ = scale;
    }

    public int getScale(){
        return scale_;
    }

    @Override
    public DATA_CATEGORY getDataCategory() {
        return DATA_CATEGORY.DENARY;
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
