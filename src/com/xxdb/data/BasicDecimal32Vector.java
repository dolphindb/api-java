package com.xxdb.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import static com.xxdb.data.Entity.DATA_TYPE.DT_DECIMAL32;

public class BasicDecimal32Vector extends AbstractVector{
    private int scale_ = -1;
    private int[] values;
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
        this.values = new int[size];

        this.size = values.length;
        capacity = values.length;
    }

    BasicDecimal32Vector(DATA_FORM df, int size){
        super(df);
        values = new int[size];

        this.size = values.length;
        capacity = values.length;
    }

    public BasicDecimal32Vector(String[] data, int scale) {
        super(DATA_FORM.DF_VECTOR);
        if (scale < 0 || scale > 9)
            throw new RuntimeException("Scale " + scale + " is out of bounds, it must be in [0,9].");
        this.scale_ = scale;

        int length = data.length;
        values = new int[length];
        BigDecimal pow = BigDecimal.TEN.pow(scale_);
        for (int i = 0; i < length; i++) {
            BigDecimal bd = new BigDecimal(data[i]);
            if (bd.multiply(pow).intValue() > Integer.MIN_VALUE && bd.multiply(pow).intValue() < Integer.MAX_VALUE) {
                values[i] = bd.multiply(pow).intValue();
            }
        }

        size = length;
        capacity = length;
    }

    BasicDecimal32Vector(int[] dataValue, int scale){
        super(DATA_FORM.DF_VECTOR);
        if (scale < 0 || scale > 9)
            throw new RuntimeException("Scale " + scale + " is out of bounds, it must be in [0,9].");
        this.scale_ = scale;
        this.values = dataValue;
        this.size = values.length;
        capacity = values.length;
    }

    public BasicDecimal32Vector(DATA_FORM df, ExtendedDataInput in, int extra) throws IOException{
        super(df);
        int rows = in.readInt();
        int cols = in.readInt();
        int size = rows * cols;
        values = new int[size];
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
                values[i + start] = value;
            }
            off += len;
        }

        this.size = values.length;
        capacity = values.length;
    }

    @Deprecated
    public BasicDecimal32Vector(double[] data, int scale){
        super(DATA_FORM.DF_VECTOR);
        if (scale < 0 || scale > 9)
            throw new RuntimeException("Scale out of bound (valid range: [0, 9], but get: " + scale + ")");
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
        values = newIntValue;
        this.size = values.length;
        capacity = values.length;
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
                values[i + start] = byteBuffer.getInt(i * 4);
            off += len;
            start += end;
        }

        this.size = values.length;
        capacity = values.length;
    }

    @Override
    protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {
        int[] data = new int[size];
        System.arraycopy(values, 0, data, 0, size);
        out.writeInt(scale_);
        out.writeIntArray(data);
    }

    @Override
    public Vector combine(Vector vector) {
        return null;
    }

    @Override
    public Vector getSubVector(int[] indices) {
        int length = indices.length;
        int[] sub = new int[length];
        for(int i=0; i<length; ++i)
            sub[i] = values[indices[i]];
        return new BasicDecimal32Vector(sub, scale_);
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
    public Entity get(int index) {
        return new BasicDecimal32(new int[]{scale_, values[index]});
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
            values[index] = Integer.MIN_VALUE;
        else{
            if(scale_ != newScale) {
                BigInteger newValue;
                if (type == Entity.DATA_TYPE.DT_LONG) {
                    newValue = BigInteger.valueOf(((BasicLong)(value)).getLong());
                } else if (type == Entity.DATA_TYPE.DT_INT) {
                    newValue = BigInteger.valueOf(((BasicInt)(value)).getInt());
                } else {
                    newValue = BigInteger.valueOf(((BasicDecimal32) (value)).getInt());
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
                values[index] = ((BasicDecimal32) value).getInt();
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
            out.writeInt(values[start + i]);
        }
    }

    @Override
    public int getUnitLength() {
        return 4;
    }

    public void add(String value) {
        if (size + 1 > capacity && values.length > 0) {
            values = Arrays.copyOf(values, values.length * 2);
        } else if (values.length <= 0){
            values = new int[1];
        }

        capacity = values.length;
        if (value.equals("0.0"))
            values[size] = 0;
        else if(value.equals(""))
            values[size] = Integer.MIN_VALUE;
        else {
            BigDecimal pow = BigDecimal.TEN.pow(scale_);
            BigDecimal bd = new BigDecimal(value);
            if (checkDecimal32Range(bd.multiply(pow).intValue()))
                values[size] = bd.multiply(pow).intValue();
        }
        size++;
    }

    @Deprecated
    public void add(double value) {
        if (size + 1 > capacity && values.length > 0){
            values = Arrays.copyOf(values, values.length * 2);
        }else if (values.length <= 0){
            values = Arrays.copyOf(values, values.length + 1);
        }
        capacity = values.length;
        if (value == 0.0)
            values[size] = 0;
        else {
            BigDecimal pow = new BigDecimal(1);
            for (long i = 0; i < scale_; i++) {
                pow = pow.multiply(new BigDecimal(10));
            }
            BigDecimal dbvalue = new BigDecimal(Double.toString(value));
            values[size] = (dbvalue.multiply(pow)).intValue();
        }
        size++;
    }

    void addRange(int[] valueList) {
        values = Arrays.copyOf(values, valueList.length + values.length);
        System.arraycopy(valueList, 0, values, size, valueList.length);
        size += valueList.length;
        capacity = values.length;
    }

    public void addRange(String[] valueList) {
        int[] newIntValue = new int[valueList.length];
        BigDecimal pow = BigDecimal.TEN.pow(scale_);
        for(int i = 0; i < valueList.length; i++){
            BigDecimal bd = new BigDecimal(valueList[i]);
            if (checkDecimal32Range(bd.multiply(pow).intValue()))
                newIntValue[i] = bd.multiply(pow).intValue();

        }
        values = Arrays.copyOf(values, newIntValue.length + values.length);
        System.arraycopy(newIntValue, 0, values, size, newIntValue.length);
        size += newIntValue.length;
        capacity = values.length;
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
        values = Arrays.copyOf(values, newIntValue.length + values.length);
        System.arraycopy(newIntValue, 0, values, size, newIntValue.length);
        size += newIntValue.length;
        capacity = values.length;
    }

    @Override
    public void Append(Scalar value) throws Exception{
        add(value.getString());
    }

    @Override
    public void Append(Vector value) throws Exception{
        if(((BasicDecimal32Vector)value).getScale() == scale_)
            addRange(((BasicDecimal32Vector)value).getdataArray());
        else{
            for(int i = 0; i < value.rows(); ++i){
                Append((Scalar)value.get(i));
            }
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
        System.arraycopy(values, 0, data, 0, size);
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
            out.putInt(values[indexStart + i]);
        }
        numElementAndPartial.numElement = targetNumElement;
        numElementAndPartial.partial = 0;
        return targetNumElement * 4;
    }

    private boolean checkDecimal32Range(int value) {
        return value > Integer.MIN_VALUE && value < Integer.MAX_VALUE;
    }
}
