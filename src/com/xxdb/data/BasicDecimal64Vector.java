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
import java.util.List;
import static com.xxdb.data.Entity.DATA_TYPE.DT_DECIMAL64;

public class BasicDecimal64Vector extends AbstractVector{
    private int scale_ = -1;
    private long[] unscaledValues;
    private int size;
    private int capacity;

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
        unscaledValues = new long[size];

        this.size = unscaledValues.length;
        capacity = unscaledValues.length;
    }

    BasicDecimal64Vector(DATA_FORM df, int size){
        super(df);
        unscaledValues = new long[size];

        this.size = unscaledValues.length;
        capacity = unscaledValues.length;
    }

    BasicDecimal64Vector(long[] dataValue, int scale){
        super(DATA_FORM.DF_VECTOR);
        if (scale < 0 || scale > 18)
            throw new RuntimeException("Scale " + scale + " is out of bounds, it must be in [0,18].");
        this.scale_ = scale;
        this.unscaledValues = dataValue;
        this.size = unscaledValues.length;
        capacity = unscaledValues.length;
    }

    public BasicDecimal64Vector(String[] data, int scale) {
        super(DATA_FORM.DF_VECTOR);
        if (scale < 0 || scale > 18)
            throw new RuntimeException("Scale " + scale + " is out of bounds, it must be in [0,18].");
        this.scale_ = scale;

        int length = data.length;
        unscaledValues = new long[length];
        for (int i = 0; i < length; i++) {
            BigDecimal bd = new BigDecimal(data[i]);
            BigDecimal multipliedValue = bd.scaleByPowerOfTen(scale).setScale(0, RoundingMode.HALF_UP);
            if (checkDecimal64Range(multipliedValue))
                unscaledValues[i] = multipliedValue.longValue();
        }

        size = length;
        capacity = length;
    }

    public BasicDecimal64Vector(DATA_FORM df, ExtendedDataInput in, int extra) throws IOException{
        super(df);
        int rows = in.readInt();
        int cols = in.readInt();
        unscaledValues = new long[rows];
        if (extra != -1)
            scale_ = extra;
        else
            scale_ = in.readInt();
        long totalBytes = (long)rows * 8;
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
                unscaledValues[i + start] = value;
            }
            off += len;
        }

        this.size = unscaledValues.length;
        capacity = unscaledValues.length;
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
        unscaledValues = newIntValue;
        this.size = unscaledValues.length;
        capacity = unscaledValues.length;
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
                unscaledValues[i + start] = byteBuffer.getLong(i * 8);
            off += len;
            start += end;
        }

        this.size = unscaledValues.length;
        capacity = unscaledValues.length;
    }

    @Override
    protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {
        long[] data = new long[size];
        System.arraycopy(unscaledValues, 0, data, 0, size);
        out.writeInt(scale_);
        out.writeLongArray(data);
    }

    @Override
    public void setExtraParamForType(int scale){
        this.scale_ = scale;
    }

    @Override
    public Vector combine(Vector vector) {
        BasicDecimal64Vector v = (BasicDecimal64Vector)vector;
        if (v.getScale() != this.scale_)
            throw new RuntimeException("The scale of the vector to be combine does not match the scale of the current vector.");
        int newSize = this.rows() + v.rows();
        long[] newValue = new long[newSize];
        System.arraycopy(this.unscaledValues,0, newValue,0,this.rows());
        System.arraycopy(v.unscaledValues,0, newValue,this.rows(),v.rows());

        return new BasicDecimal64Vector(newValue, this.scale_);
    }

    @Override
    public Vector getSubVector(int[] indices) {
        int length = indices.length;
        long[] sub = new long[length];
        for(int i=0; i<length; ++i)
            sub[i] = unscaledValues[indices[i]];
        return new BasicDecimal64Vector(sub, scale_);
    }

    @Override
    public int asof(Scalar value) {
        return 0;
    }

    @Override
    public boolean isNull(int index) {
        return unscaledValues[index] == Long.MIN_VALUE;
    }

    @Override
    public void setNull(int index) {
        unscaledValues[index] = Long.MIN_VALUE;
    }

    @Override
    public Entity get(int index) {
        return new BasicDecimal64(scale_, unscaledValues[index]);
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
            unscaledValues[index] = Long.MIN_VALUE;
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
                unscaledValues[index] = newValue.intValue();
            }else{
                unscaledValues[index] = ((BasicDecimal64) value).getLong();
            }
        }
    }

    @Override
    public void set(int index, Object value) {
        if (value == null) {
            setNull(index);
        } else if (value instanceof String) {
            try {
                BigDecimal bd = new BigDecimal((String)value);
                if (checkDecimal64Range(bd)) {
                    BigDecimal multipliedValue = bd.scaleByPowerOfTen(scale_).setScale(0, RoundingMode.HALF_UP);
                    unscaledValues[index] = multipliedValue.longValue();
                } else {
                    setNull(index);
                }
            } catch (Exception e) {
                setNull(index);
            }
        } else if (value instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) value;
            if (checkDecimal64Range(bd)) {
                BigDecimal multipliedValue = bd.scaleByPowerOfTen(scale_).setScale(0, RoundingMode.HALF_UP);
                unscaledValues[index] = multipliedValue.longValue();
            } else {
                setNull(index);
            }
        } else {
            throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName() + ". Only String, BigDecimal or null is supported.");
        }
    }

    @Override
    public Class<?> getElementClass() {
        return BasicDecimal64.class;
    }

    @Override
    public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {
        for (int i = 0; i < count; i++){
            out.writeLong(unscaledValues[start + i]);
        }
    }

    @Override
    public int getUnitLength() {
        return 8;
    }

    @Override
    public void add(Object value) {
        if (value == null) {
            if (size + 1 > capacity && unscaledValues.length > 0) {
                unscaledValues = Arrays.copyOf(unscaledValues, unscaledValues.length * 2);
            } else if (unscaledValues.length <= 0) {
                unscaledValues = new long[1];
            }
            capacity = unscaledValues.length;
            unscaledValues[size] = Long.MIN_VALUE;
            size++;
        } else if (value instanceof String) {
            add((String) value);
        } else if (value instanceof BigDecimal) {
            if (size + 1 > capacity && unscaledValues.length > 0) {
                unscaledValues = Arrays.copyOf(unscaledValues, unscaledValues.length * 2);
            } else if (unscaledValues.length <= 0) {
                unscaledValues = new long[1];
            }
            capacity = unscaledValues.length;

            BigDecimal bd = (BigDecimal) value;
            BigDecimal pow = BigDecimal.TEN.pow(scale_);
            if (checkDecimal64Range(bd)) {
                unscaledValues[size] = bd.multiply(pow).longValue();
            }
            size++;
        } else {
            throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName() + ". Only String, BigDecimal or null is supported.");
        }
    }

    public void add(String value) {
        if (size + 1 > capacity && unscaledValues.length > 0) {
            unscaledValues = Arrays.copyOf(unscaledValues, unscaledValues.length * 2);
        } else if (unscaledValues.length <= 0) {
            unscaledValues = new long[1];
        }

        capacity = unscaledValues.length;
        if (value.equals("0.0"))
            unscaledValues[size] = 0;
        else if(value.equals(""))
            unscaledValues[size] = Long.MIN_VALUE;
        else {
            BigDecimal pow = BigDecimal.TEN.pow(scale_);
            BigDecimal bd = new BigDecimal(value);
            if (checkDecimal64Range(bd))
                unscaledValues[size] = bd.multiply(pow).longValue();
        }
        size++;
    }

    @Deprecated
    public void add(double value) {
        if (size + 1 > capacity && unscaledValues.length > 0){
            unscaledValues = Arrays.copyOf(unscaledValues, unscaledValues.length * 2);
        }else if (unscaledValues.length <= 0){
            unscaledValues = Arrays.copyOf(unscaledValues, unscaledValues.length + 1);
        }
        capacity = unscaledValues.length;
        if (value == 0.0)
            unscaledValues[size] = 0;
        else {
            BigDecimal pow = new BigDecimal(1);
            for (long i = 0; i < scale_; i++) {
                pow = pow.multiply(new BigDecimal(10));
            }
            BigDecimal dbvalue = new BigDecimal(Double.toString(value));
            unscaledValues[size] = (dbvalue.multiply(pow)).longValue();
        }
        size++;
    }


    void addRange(long[] valueList) {
        int requiredCapacity = size + valueList.length;
        checkCapacity(requiredCapacity);
        System.arraycopy(valueList, 0, unscaledValues, size, valueList.length);
        size += valueList.length;
    }

    public void addRange(String[] valueList) {
        long[] newLongValue = new long[valueList.length];
        BigDecimal pow = BigDecimal.TEN.pow(scale_);
        for (int i = 0; i < valueList.length; i++) {
            BigDecimal bd = new BigDecimal(valueList[i]);
            if (checkDecimal64Range(bd))
                newLongValue[i] = bd.multiply(pow).longValue();
        }
        unscaledValues = Arrays.copyOf(unscaledValues, newLongValue.length + unscaledValues.length);
        System.arraycopy(newLongValue, 0, unscaledValues, size, newLongValue.length);
        size += newLongValue.length;
        capacity = unscaledValues.length;
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
        unscaledValues = Arrays.copyOf(unscaledValues, newLongValue.length + unscaledValues.length);
        System.arraycopy(newLongValue, 0, unscaledValues, size, newLongValue.length);
        size += newLongValue.length;
        capacity = unscaledValues.length;
    }

    @Override
    public void Append(Scalar value) throws Exception{
        add(value.getString());
    }

    @Override
    public void Append(Vector value) throws Exception{
        if (((BasicDecimal64Vector)value).getScale() == scale_)
            addRange(((BasicDecimal64Vector) value).getdataArray());
        else {
            for(int i = 0; i < value.rows(); ++i)
                Append((Scalar)value.get(i));
        }
    }

    @Override
    public void checkCapacity(int requiredCapacity) {
        if (requiredCapacity > unscaledValues.length) {
            int newCapacity = Math.max(
                    (int)(unscaledValues.length * GROWTH_FACTOR),
                    requiredCapacity
            );
            unscaledValues = Arrays.copyOf(unscaledValues, newCapacity);
            capacity = newCapacity;
        }
    }

    @JsonIgnore
    public long[] getdataArray(){
        long[] data = new long[size];
        System.arraycopy(unscaledValues, 0, data, 0, size);
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
            out.putLong(unscaledValues[indexStart + i]);
        }
        numElementAndPartial.numElement = targetNumElement;
        numElementAndPartial.partial = 0;
        return targetNumElement * 8;
    }

    private boolean checkDecimal64Range(BigDecimal value) {
        return value.compareTo(DECIMAL64_MIN_VALUE) > 0 && value.compareTo(DECIMAL64_MAX_VALUE) < 0;
    }

    public long[] getUnscaledValues() {
        return unscaledValues;
    }
}
