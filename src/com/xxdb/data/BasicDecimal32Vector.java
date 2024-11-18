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
import static com.xxdb.data.Entity.DATA_TYPE.DT_DECIMAL32;

public class BasicDecimal32Vector extends AbstractVector{
    private int scale_ = -1;
    private int[] unscaledValues;
    private int size;
    private int capacity;

    public BasicDecimal32Vector(int size){
        this(DATA_FORM.DF_VECTOR, size);
    }

    public BasicDecimal32Vector(int size, int scale){
        super(DATA_FORM.DF_VECTOR);
        if (scale < 0 || scale > 9)
            throw new RuntimeException("Scale " + scale + " is out of bounds, it must be in [0,9].");
        this.scale_ = scale;
        this.unscaledValues = new int[size];

        this.size = unscaledValues.length;
        capacity = unscaledValues.length;
    }

    BasicDecimal32Vector(DATA_FORM df, int size){
        super(df);
        unscaledValues = new int[size];

        this.size = unscaledValues.length;
        capacity = unscaledValues.length;
    }

    public BasicDecimal32Vector(String[] data, int scale) {
        super(DATA_FORM.DF_VECTOR);
        if (scale < 0 || scale > 9)
            throw new RuntimeException("Scale " + scale + " is out of bounds, it must be in [0,9].");
        this.scale_ = scale;

        int length = data.length;
        unscaledValues = new int[length];
        for (int i = 0; i < length; i++) {
            BigDecimal bd = new BigDecimal(data[i]);
            BigDecimal multipliedValue = bd.scaleByPowerOfTen(scale).setScale(0, RoundingMode.HALF_UP);
            if (multipliedValue.intValue() > Integer.MIN_VALUE && multipliedValue.intValue() < Integer.MAX_VALUE)
                unscaledValues[i] = multipliedValue.intValue();
        }

        size = length;
        capacity = length;
    }

    BasicDecimal32Vector(int[] dataValue, int scale){
        super(DATA_FORM.DF_VECTOR);
        if (scale < 0 || scale > 9)
            throw new RuntimeException("Scale " + scale + " is out of bounds, it must be in [0,9].");
        this.scale_ = scale;
        this.unscaledValues = dataValue;
        this.size = unscaledValues.length;
        capacity = unscaledValues.length;
    }

    public BasicDecimal32Vector(DATA_FORM df, ExtendedDataInput in, int extra) throws IOException{
        super(df);
        int rows = in.readInt();
        int cols = in.readInt();
        int size = rows * cols;
        unscaledValues = new int[size];
        if (extra != -1)
            scale_ = extra;
        else
            scale_ = in.readInt();
        long totalBytes = (long)size * 4;
        long off = 0;
        boolean little = in.isLittleEndian();
        ByteOrder bo = little ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
        byte[] buf = new byte[4096];
        while (off < totalBytes){
            int len = (int)Math.min(4096, totalBytes - off);
            in.readFully(buf, 0, len);
            int start = (int)(off / 4), end = len / 4;
            ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
            for (int i = 0; i < end; i++){
                int value = byteBuffer.getInt(i * 4);
                unscaledValues[i + start] = value;
            }
            off += len;
        }

        this.size = unscaledValues.length;
        capacity = unscaledValues.length;
    }

    @Deprecated
    public BasicDecimal32Vector(double[] data, int scale){
        super(DATA_FORM.DF_VECTOR);
        if (scale < 0 || scale > 9)
            throw new RuntimeException("Scale " + scale + " is out of bounds, it must be in [0,9].");
        this.scale_ = scale;
        int[] newIntValue = new int[data.length];
        for(int i = 0; i < data.length; i++){
            BigDecimal pow = new BigDecimal(1);
            for (long j = 0; j < scale_; j++) {
                pow = pow.multiply(new BigDecimal(10));
            }
            BigDecimal dbvalue = new BigDecimal(Double.toString(data[i]));
            newIntValue[i] = (dbvalue.multiply(pow)).intValue();
        }
        unscaledValues = newIntValue;
        this.size = unscaledValues.length;
        capacity = unscaledValues.length;
    }

    @Override
    public void deserialize(int start, int count, ExtendedDataInput in) throws IOException{
        long totalBytes = (long)count * 4, off = 0;
        ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
        byte[] buf = new byte[4096];
        while (off < totalBytes) {
            int len = (int)Math.min(4096, totalBytes - off);
            in.readFully(buf, 0, len);
            int end = len / 4;
            ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
            for (int i = 0; i < end; i++)
                unscaledValues[i + start] = byteBuffer.getInt(i * 4);
            off += len;
            start += end;
        }

        this.size = unscaledValues.length;
        capacity = unscaledValues.length;
    }

    @Override
    protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {
        int[] data = new int[size];
        System.arraycopy(unscaledValues, 0, data, 0, size);
        out.writeInt(scale_);
        out.writeIntArray(data);
    }

    @Override
    public Vector combine(Vector vector) {
        BasicDecimal32Vector v = (BasicDecimal32Vector)vector;
        if (v.getScale() != this.scale_)
            throw new RuntimeException("The scale of the vector to be combine does not match the scale of the current vector.");
        int newSize = this.rows() + v.rows();
        int[] newValue = new int[newSize];
        System.arraycopy(this.unscaledValues,0, newValue,0,this.rows());
        System.arraycopy(v.unscaledValues,0, newValue,this.rows(),v.rows());

        return new BasicDecimal32Vector(newValue, this.scale_);
    }

    @Override
    public Vector getSubVector(int[] indices) {
        int length = indices.length;
        int[] sub = new int[length];
        for(int i=0; i<length; ++i)
            sub[i] = unscaledValues[indices[i]];
        return new BasicDecimal32Vector(sub, scale_);
    }

    @Override
    public int asof(Scalar value) {
        return 0;
    }

    @Override
    public boolean isNull(int index) {
        return unscaledValues[index] == Integer.MIN_VALUE;
    }

    @Override
    public void setNull(int index) {
        unscaledValues[index] = Integer.MIN_VALUE;
    }

    @Override
    public Entity get(int index) {
        return new BasicDecimal32(new int[]{scale_, unscaledValues[index]});
    }

    @Override
    public void set(int index, Entity value) throws Exception {
        if (!value.getDataForm().equals(DATA_FORM.DF_SCALAR) || value.getDataType() != DT_DECIMAL32) {
            throw new RuntimeException("value type is not BasicDecimal32!");
        }

        int newScale = ((Scalar) value).getScale();
        DATA_TYPE type = value.getDataType();
        if(scale_ < 0) scale_ = newScale;
        if(((Scalar)value).isNull())
            unscaledValues[index] = Integer.MIN_VALUE;
        else{
            if(scale_ != newScale) {
                BigInteger newValue = BigInteger.valueOf(((BasicDecimal32) (value)).getInt());
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
                unscaledValues[index] = ((BasicDecimal32) value).getInt();
            }
        }
    }

    @Override
    public Class<?> getElementClass() {
        return BasicDecimal32.class;
    }

    @Override
    public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {
        for (int i = 0; i < count; i++){
            out.writeInt(unscaledValues[start + i]);
        }
    }

    @Override
    public int getUnitLength() {
        return 4;
    }

    public void add(String value) {
        if (size + 1 > capacity && unscaledValues.length > 0) {
            unscaledValues = Arrays.copyOf(unscaledValues, unscaledValues.length * 2);
        } else if (unscaledValues.length <= 0){
            unscaledValues = new int[1];
        }

        capacity = unscaledValues.length;
        if (value.equals("0.0"))
            unscaledValues[size] = 0;
        else if(value.equals(""))
            unscaledValues[size] = Integer.MIN_VALUE;
        else {
            BigDecimal pow = BigDecimal.TEN.pow(scale_);
            BigDecimal bd = new BigDecimal(value);
            if (checkDecimal32Range(bd.multiply(pow).intValue()))
                unscaledValues[size] = bd.multiply(pow).intValue();
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
            unscaledValues[size] = (dbvalue.multiply(pow)).intValue();
        }
        size++;
    }

    void addRange(int[] valueList) {
        int requiredCapacity = size + valueList.length;
        checkCapacity(requiredCapacity);
        System.arraycopy(valueList, 0, unscaledValues, size, valueList.length);
        size += valueList.length;
    }

    public void addRange(String[] valueList) {
        int[] newIntValue = new int[valueList.length];
        BigDecimal pow = BigDecimal.TEN.pow(scale_);
        for(int i = 0; i < valueList.length; i++){
            BigDecimal bd = new BigDecimal(valueList[i]);
            if (checkDecimal32Range(bd.multiply(pow).intValue()))
                newIntValue[i] = bd.multiply(pow).intValue();

        }
        unscaledValues = Arrays.copyOf(unscaledValues, newIntValue.length + unscaledValues.length);
        System.arraycopy(newIntValue, 0, unscaledValues, size, newIntValue.length);
        size += newIntValue.length;
        capacity = unscaledValues.length;
    }

    @Deprecated
    public void addRange(double[] valueList) {
        int[] newIntValue = new int[valueList.length];
        for(int i = 0; i < valueList.length; i++){
            BigDecimal pow = new BigDecimal(1);
            for (long j = 0; j < scale_; j++) {
                pow = pow.multiply(new BigDecimal(10));
            }
            BigDecimal dbvalue = new BigDecimal(Double.toString(valueList[i]));
            newIntValue[i] = (dbvalue.multiply(pow)).intValue();
        }
        unscaledValues = Arrays.copyOf(unscaledValues, newIntValue.length + unscaledValues.length);
        System.arraycopy(newIntValue, 0, unscaledValues, size, newIntValue.length);
        size += newIntValue.length;
        capacity = unscaledValues.length;
    }

    @Override
    public void Append(Scalar value) throws Exception{
        add(value.getString());
    }

    @Override
    public void Append(Vector value) throws Exception{
        if (((BasicDecimal32Vector)value).getScale() == scale_)
            addRange(((BasicDecimal32Vector) value).getdataArray());
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

    public void setScale(int scale){
        this.scale_ = scale;
    }

    public int getScale(){
        return scale_;
    }

    @JsonIgnore
    public int[] getdataArray(){
        int[] data = new int[size];
        System.arraycopy(unscaledValues, 0, data, 0, size);
        return data;
    }

    @Override
    public void setExtraParamForType(int scale){
        this.scale_ = scale;
    }

    @Override
    public DATA_CATEGORY getDataCategory() {
        return DATA_CATEGORY.DENARY;
    }

    @Override
    public DATA_TYPE getDataType() {
        return DATA_TYPE.DT_DECIMAL32;
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
            out.putInt(unscaledValues[indexStart + i]);
        }
        numElementAndPartial.numElement = targetNumElement;
        numElementAndPartial.partial = 0;
        return targetNumElement * 4;
    }

    private boolean checkDecimal32Range(int value) {
        return value > Integer.MIN_VALUE && value < Integer.MAX_VALUE;
    }

    public int[] getUnscaledValues() {
        return unscaledValues;
    }
}
