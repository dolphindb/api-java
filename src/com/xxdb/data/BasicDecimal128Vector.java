package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class BasicDecimal128Vector extends AbstractVector {

    private static final BigDecimal DECIMAL128_MIN_VALUE = new BigDecimal("-170141183460469231731687303715884105728");

    private int scale_ = -1;
    private BigInteger[] values;
    private int size;
    private int capacity;

    public BasicDecimal128Vector(int size) {
        this(DATA_FORM.DF_VECTOR, size);
    }

    public BasicDecimal128Vector(int size, int scale) {
        super(DATA_FORM.DF_VECTOR);
        this.scale_ = scale;
        values = new BigInteger[size];
        Arrays.fill(values, BigInteger.ZERO);

        this.size = values.length;
        capacity = values.length;
    }

    BasicDecimal128Vector(DATA_FORM df, int size) {
        super(df);
        values = new BigInteger[size];
        Arrays.fill(values, BigInteger.ZERO);
        this.size = values.length;
        capacity = values.length;
    }

    BasicDecimal128Vector(BigInteger[] dataValue, int scale) {
        super(DATA_FORM.DF_VECTOR);
        this.scale_ = scale;
        BigInteger[] newArray = new BigInteger[dataValue.length];
        System.arraycopy(dataValue, 0, newArray, 0, dataValue.length);
        this.values = newArray;
        this.size = values.length;
        capacity = values.length;
    }

    public BasicDecimal128Vector(DATA_FORM df, ExtendedDataInput in, int extra) throws IOException {
        super(df);
        int rows = in.readInt();
        int cols = in.readInt();
        int size = rows * cols;
        values = new BigInteger[size];
        if (extra != -1)
            scale_ = extra;
        else
            scale_ = in.readInt();
        for (int i = 0; i < size; i++) {
            values[i] = handleLittleEndianBigEndian(in);
        }
        this.size = values.length;
        capacity = values.length;
    }

    private static BigInteger handleLittleEndianBigEndian(ExtendedDataInput in) throws IOException {
        byte[] buffer = new byte[16];
        BigInteger value;

        if (in.isLittleEndian()) {
            for (int i = buffer.length-1; i >=0; i--) {
                buffer[i] = in.readByte();
            }
            value = new BigInteger(buffer);
        } else {
            for (int i = 0; i < buffer.length; i++) {
                buffer[i] = in.readByte();
            }
            value = new BigInteger(buffer);
        }

        return value;
    }

    public BasicDecimal128Vector(double[] data, int scale) {
        super(DATA_FORM.DF_VECTOR);
        if (scale < 0 || scale > 18)
            throw new RuntimeException("Scale out of bound (valid range: [0, 18], but get: " + scale + ")");
        scale_ = scale;
        BigInteger[] newValues = new BigInteger[data.length];
        for (int i = 0; i < data.length; i++) {
            BigDecimal pow = BigDecimal.TEN.pow(scale);
            BigDecimal bdValue = new BigDecimal(data[i]);
            BigDecimal scaledValue = bdValue.multiply(pow);
            newValues[i] = scaledValue.toBigInteger();
        }
        values = newValues;
        this.size = values.length;
        capacity = values.length;
    }

    @Override
    public void deserialize(int start, int count, ExtendedDataInput in) throws IOException {
        if (capacity < start + count) {
            BigInteger[] expandedArray = new BigInteger[start + count];
            System.arraycopy(values, 0, expandedArray, 0, values.length);
            values = expandedArray;
			this.size = start + count;
            capacity = start + count;
		} else if (this.size < start + count) {
			this.size = start + count;
		}

        for (int i = 0; i < count; i++) {
            values[start + i] = handleLittleEndianBigEndian(in);
        }
    }

    @Override
    public void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {
        out.writeInt(scale_);
        byte[] newArray = new byte[16 * size];
        for (int i = 0; i < size; i++ ) {
            BigInteger value = values[i];
            byte[] originalArray = value.toByteArray();

            if (((originalArray[0] >> 7) & 1) == 0) {
                // if first bit is 0, represent non-negative.
                System.arraycopy(originalArray, 0, newArray, (16 - originalArray.length) + 16*i, originalArray.length);
            } else {
                // if first bit is 1, represent negative.
                System.arraycopy(originalArray, 0, newArray, 16 - originalArray.length, originalArray.length);
                for (int j = 0; j < 16 - originalArray.length; j++) {
                    newArray[j] = -1;
                }
            }
        }

        reverseByteArrayEvery8Byte(newArray);
        out.write(newArray);
    }

    public static void reverseByteArray(byte[] array) {
        int left = 0;
        int right = array.length - 1;

        while (left < right) {
            byte temp = array[left];
            array[left] = array[right];
            array[right] = temp;

            left++;
            right--;
        }
    }

    public static void reverseByteArrayEvery8Byte(byte[] array) {
        int start = 0;
        int end = start + 15;

        while (end < array.length) {
            for (int i = 0; i < 8; i++) {
                byte temp = array[start + i];
                array[start + i] = array[end - i];
                array[end - i] = temp;
            }

            start += 16;
            end += 16;
        }
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
        BigInteger[] sub = new BigInteger[length];
        for (int i = 0; i < length; i++)
            sub[i] = values[indices[i]];
        return new BasicDecimal128Vector(sub, scale_);
    }

    @Override
    public int asof(Scalar value) {
        return 0;
    }

    @Override
    public boolean isNull(int index) {
        return values[index].equals(DECIMAL128_MIN_VALUE.toBigInteger());
    }

    @Override
    public void setNull(int index) {
        values[index] = DECIMAL128_MIN_VALUE.toBigInteger();
    }

    @Override
    public Entity get(int index) {
        return new BasicDecimal128(values[index], scale_);
    }

    @Override
    public void set(int index, Entity value) throws Exception {
        int newScale = ((Scalar) value).getScale();
        DATA_TYPE type = value.getDataType();
        if (scale_ < 0)
            scale_ = newScale;
        if (((Scalar) value).isNull()) {
            values[index] = DECIMAL128_MIN_VALUE.toBigInteger();
        } else {
            if (scale_ != newScale) {
                BigInteger newValue;
                if (type == Entity.DATA_TYPE.DT_LONG) {
                    newValue = BigInteger.valueOf(((BasicLong) value).getLong());
                } else if (type == Entity.DATA_TYPE.DT_INT) {
                    newValue = BigInteger.valueOf(((BasicInt) value).getInt());
                } else {
                    newValue = ((BasicDecimal128) value).getBigInteger();
                }

                BigInteger pow = BigInteger.TEN;
                if (newScale - scale_ > 0) {
                    pow = pow.pow(newScale - scale_);
                    newValue = newValue.divide(pow);
                } else {
                    pow = pow.pow(scale_ - newScale);
                    newValue = newValue.multiply(pow);
                }
                values[index] = newValue;
            } else {
                values[index] = ((BasicDecimal128) value).getBigInteger();
            }
        }
    }

    @Override
    public Class<?> getElementClass() {
        return BasicDecimal128.class;
    }

    @Override
    public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {
        for (int i = 0; i < count; i++) {
            BigInteger value = values[start + i];
            byte[] newArray = new byte[16];
            byte[] originalArray = value.toByteArray();

            if (((originalArray[0] >> 7) & 1) == 0) {
                // if first bit is 0, represent non-negative.
                System.arraycopy(originalArray, 0, newArray, 16 - originalArray.length, originalArray.length);
            } else {
                // if first bit is 1, represent negative.
                System.arraycopy(originalArray, 0, newArray, 16 - originalArray.length, originalArray.length);
                for (int j = 0; j < 16 - originalArray.length; j++) {
                    newArray[j] = -1;
                }
            }

            reverseByteArray(newArray);
            out.write(newArray);
        }
    }

    @Override
    public int getUnitLength() {
        return 16;
    }

    public void add(double value) {
        if (scale_ < 0) {
            throw new RuntimeException("Please set scale first.");
        }
        if (size + 1 > capacity && values.length > 0) {
            values = Arrays.copyOf(values, values.length * 2);
        } else if (values.length <= 0) {
            values = Arrays.copyOf(values, values.length + 1);
        }
        capacity = values.length;
        if (value == 0.0) {
            values[size] = BigInteger.ZERO;
        } else {
            BigDecimal pow = BigDecimal.TEN.pow(scale_);
            BigDecimal dbValue = BigDecimal.valueOf(value);
            BigInteger scaledValue = dbValue.multiply(pow).toBigInteger();
            values[size] = scaledValue;
        }
        size++;
    }

    void addRange(BigInteger[] valueList) {
        int newSize = size + valueList.length;
        if (newSize > capacity && values.length > 0) {
            values = Arrays.copyOf(values, Math.max(values.length * 2, newSize));
        } else if (values.length <= 0) {
            values = Arrays.copyOf(values, valueList.length);
        }
        System.arraycopy(valueList, 0, values, size, valueList.length);
        size = newSize;
        capacity = values.length;
    }

    public void addRange(double[] valueList) {
        if (scale_ < 0) {
            throw new RuntimeException("Please set scale first.");
        }

        BigInteger[] newValues = new BigInteger[valueList.length];
        for (int i = 0; i < valueList.length; i++) {
            BigDecimal pow = BigDecimal.ONE;
            for (int j = 0; j < scale_; j++) {
                pow = pow.multiply(BigDecimal.TEN);
            }
            BigDecimal dbValue = BigDecimal.valueOf(valueList[i]);
            BigInteger scaledValue = dbValue.multiply(pow).toBigInteger();
            newValues[i] = scaledValue;
        }

        int newSize = size + newValues.length;
        if (newSize > capacity && values.length > 0) {
            values = Arrays.copyOf(values, Math.max(values.length * 2, newSize));
        } else if (values.length <= 0) {
            values = Arrays.copyOf(values, newValues.length);
        }
        System.arraycopy(newValues, 0, values, size, newValues.length);
        size = newSize;
        capacity = values.length;
    }

    @Override
    public void Append(Scalar value) throws Exception{
        if (scale_ < 0)
            throw new RuntimeException("Please set scale first.");
        add(value.getNumber().doubleValue());
    }

    @Override
    public void Append(Vector value) throws Exception {
        if (scale_ < 0) {
            throw new RuntimeException("Please set scale first.");
        }
        if (value instanceof BasicDecimal128Vector && ((BasicDecimal128Vector) value).getScale() == scale_) {
            addRange(((BasicDecimal128Vector) value).getdataArray());
        } else {
            for (int i = 0; i < value.rows(); i++) {
                Scalar scalarValue = (Scalar) value.get(i);
                Append(scalarValue);
            }
        }
    }

    public BigInteger[] getdataArray() {
        BigInteger[] data = new BigInteger[size];
        for (int i = 0; i < size; i++) {
            data[i] = values[i];
        }

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
        return DATA_TYPE.DT_DECIMAL128;
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
    public int serialize(int indexStart, int offect, int targetNumElement, NumElementAndPartial numElementAndPartial,  ByteBuffer out) throws IOException{

        int unitLength = 16;

        targetNumElement = Math.min((out.remaining() / unitLength), targetNumElement);
        for (int i = 0; i < targetNumElement; ++i) {
            byte[] newArray = new byte[16];
            byte[] originalArray = values[indexStart + i].toByteArray();
            if (originalArray.length > unitLength) {
                throw new IOException("BigInteger value exceeds 16 bytes");
            }

            if (((originalArray[0] >> 7) & 1) == 0) {
                // if first bit is 0, represent non-negative.
                System.arraycopy(originalArray, 0, newArray, 16 - originalArray.length, originalArray.length);
            } else {
                // if first bit is 1, represent negative.
                System.arraycopy(originalArray, 0, newArray, 16 - originalArray.length, originalArray.length);
                for (int j = 0; j < 16 - originalArray.length; j++) {
                    newArray[j] = -1;
                }
            }

            reverseByteArray(newArray);
            out.put(newArray);
        }
        numElementAndPartial.numElement = targetNumElement;
        numElementAndPartial.partial = 0;
        return targetNumElement * unitLength;
    }
}

