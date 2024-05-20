package com.xxdb.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import static com.xxdb.data.Entity.DATA_TYPE.DT_DECIMAL64;

public class BasicDecimal64Vector extends AbstractVector{
    private int scale_ = -1;
    private long[] values;
    private int size;
    private int capaticy;

    private static final BigDecimal DECIMAL64_MIN_VALUE = new BigDecimal("-9223372036854775808");
    private static final BigDecimal DECIMAL64_MAX_VALUE = new BigDecimal("9223372036854775807");

    public BasicDecimal64Vector(int size){
        this(DATA_FORM.DF_VECTOR, size);
    }

    public BasicDecimal64Vector(int size, int scale){
        super(DATA_FORM.DF_VECTOR);
        if (scale < 0 || scale > 18)
            throw new RuntimeException("Scale " + scale + " is out of bounds, it must be in [0,18].");
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
        if (scale < 0 || scale > 18)
            throw new RuntimeException("Scale " + scale + " is out of bounds, it must be in [0,18].");
        this.scale_ = scale;
        this.values = dataValue;
        this.size = values.length;
        capaticy = values.length;
    }

    public BasicDecimal64Vector(String[] data, int scale) {
        super(DATA_FORM.DF_VECTOR);
        if (scale < 0 || scale > 18)
            throw new RuntimeException("Scale " + scale + " is out of bounds, it must be in [0,18].");
        this.scale_ = scale;

        int length = data.length;
        values = new long[length];
        for (int i = 0; i < length; i++) {
            BigDecimal bd = new BigDecimal(data[i]);
            BigDecimal multipliedValue = bd.scaleByPowerOfTen(scale).setScale(0, RoundingMode.HALF_UP);
            if (checkDecimal64Range(multipliedValue))
                values[i] = multipliedValue.longValue();
        }

        size = length;
        capaticy = length;
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

    @Deprecated
    public BasicDecimal64Vector(double[] data, int scale){
        super(DATA_FORM.DF_VECTOR);
        if (scale < 0 || scale > 18)
            throw new RuntimeException("Scale " + scale + " is out of bounds, it must be in [0,18].");
        this.scale_ = scale;
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
        if (!value.getDataForm().equals(DATA_FORM.DF_SCALAR) || value.getDataType() != DT_DECIMAL64) {
            throw new RuntimeException("value type is not BasicDecimal64!");
        }

        int newScale = ((Scalar) value).getScale();
        DATA_TYPE type = value.getDataType();
        if(scale_ < 0) scale_ = newScale;
        if(((Scalar)value).isNull())
            values[index] = Long.MIN_VALUE;
        else{
            if(scale_ != newScale) {
                BigInteger newValue;
                if (type == Entity.DATA_TYPE.DT_LONG) {
                    newValue = BigInteger.valueOf(((BasicLong)(value)).getLong());
                } else if (type == Entity.DATA_TYPE.DT_INT) {
                    newValue = BigInteger.valueOf(((BasicInt)(value)).getInt());
                } else {
                    newValue = BigInteger.valueOf(((BasicDecimal64) (value)).getLong());
                }

                BigInteger pow = BigInteger.valueOf(10);
                if(newScale - scale_ > 0){
                    pow = pow.pow(newScale - scale_);
                    newValue = newValue.divide(pow);
                }else{
                    pow = pow.pow(scale_ - newScale);
                    newValue = newValue.multiply(pow);
                }
                values[index] = newValue.intValue();
            }else{
                values[index] = ((BasicDecimal64) value).getLong();
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

    public void add(String value) {
        if (size + 1 > capaticy && values.length > 0) {
            values = Arrays.copyOf(values, values.length * 2);
        } else if (values.length <= 0) {
            values = new long[1];
        }

        capaticy = values.length;
        if (value.equals("0.0"))
            values[size] = 0;
        else if(value.equals(""))
            values[size] = Long.MIN_VALUE;
        else {
            BigDecimal pow = BigDecimal.TEN.pow(scale_);
            BigDecimal bd = new BigDecimal(value);
            if (checkDecimal64Range(bd))
                values[size] = bd.multiply(pow).longValue();
        }
        size++;
    }

    @Deprecated
    public void add(double value) {
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

    public void addRange(String[] valueList) {
        long[] newLongValue = new long[valueList.length];
        BigDecimal pow = BigDecimal.TEN.pow(scale_);
        for (int i = 0; i < valueList.length; i++) {
            BigDecimal bd = new BigDecimal(valueList[i]);
            if (checkDecimal64Range(bd))
                newLongValue[i] = bd.multiply(pow).longValue();
        }
        values = Arrays.copyOf(values, newLongValue.length + values.length);
        System.arraycopy(newLongValue, 0, values, size, newLongValue.length);
        size += newLongValue.length;
        capaticy = values.length;
    }

    @Deprecated
    public void addRange(double[] valueList) {
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
        add(value.getString());
    }

    @Override
    public void Append(Vector value) throws Exception{
        if(((BasicDecimal64Vector)value).getScale() == scale_)
            addRange(((BasicDecimal64Vector)value).getdataArray());
        else{
            for(int i = 0; i < value.rows(); ++i){
                Append((Scalar)value.get(i));
            }
        }
    }

    @JsonIgnore
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

    private boolean checkDecimal64Range(BigDecimal value) {
        return value.compareTo(DECIMAL64_MIN_VALUE) > 0 && value.compareTo(DECIMAL64_MAX_VALUE) < 0;
    }

    public long[] getValues() {
        return values;
    }
}
