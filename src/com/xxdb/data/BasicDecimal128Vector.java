package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;
import static com.xxdb.data.Entity.DATA_TYPE.DT_DECIMAL128;

public class BasicDecimal128Vector extends AbstractVector {

    private static final BigDecimal DECIMAL128_MIN_VALUE = new BigDecimal("-170141183460469231731687303715884105728");
    private static final BigDecimal DECIMAL128_MAX_VALUE = new BigDecimal("170141183460469231731687303715884105728");
    private static final BigInteger BIGINT_MIN_VALUE = new BigInteger("-170141183460469231731687303715884105728");
    private static final BigInteger BIGINT_MAX_VALUE = new BigInteger("170141183460469231731687303715884105728");

    private int scale_ = -1;
    private BigInteger[] unscaledValues;
    private int size;
    private int capacity;

    public BasicDecimal128Vector(int size, int scale) {
        super(DATA_FORM.DF_VECTOR);
        this.scale_ = scale;
        this.unscaledValues = new BigInteger[size];
        Arrays.fill(this.unscaledValues, BigInteger.ZERO);

        this.size = this.unscaledValues.length;
        this.capacity = this.unscaledValues.length;
    }

    public BasicDecimal128Vector(String[] dataValue, int scale) {
        super(DATA_FORM.DF_VECTOR);
        if (scale < 0 || scale > 38) {
            throw new RuntimeException("Scale out of bound (valid range: [0, 38], but get: " + scale + ")");
        }
        this.scale_ = scale;

        BigInteger[] newArray = new BigInteger[dataValue.length];
        for (int i = 0; i < dataValue.length; i++ ) {
            BigDecimal bd = new BigDecimal(dataValue[i]).scaleByPowerOfTen(scale);
            BigInteger unscaledValue = bd.toBigInteger();

            if (bd.compareTo(DECIMAL128_MIN_VALUE) <0 || bd.compareTo(DECIMAL128_MAX_VALUE) > 0) {
                throw new RuntimeException("Decimal128 overflow " + unscaledValue);
            }

            newArray[i] = unscaledValue;
            if (newArray[i].compareTo(BIGINT_MIN_VALUE) < 0) {
                throw new RuntimeException("Decimal128 below " + newArray[i]);
            }

            if (newArray[i].compareTo(BIGINT_MAX_VALUE) > 0) {
                throw new RuntimeException("Decimal128 overflow " + newArray[i]);
            }
        }
        this.unscaledValues = newArray;
        this.size = this.unscaledValues.length;
        this.capacity = this.unscaledValues.length;
    }

    BasicDecimal128Vector(BigInteger[] dataValue, int scale) {
        super(DATA_FORM.DF_VECTOR);
        this.scale_ = scale;
        BigInteger[] newArray = new BigInteger[dataValue.length];
        for(int i = 0; i < dataValue.length; i++ ) {
            if (Objects.isNull(dataValue[i])) {
                newArray[i] = BigInteger.ZERO;
            } else {
                newArray[i] = checkValRange(dataValue[i]);
            }
        }
        this.unscaledValues = newArray;
        this.size = this.unscaledValues.length;
        this.capacity = this.unscaledValues.length;
    }

    private BigInteger checkValRange(BigInteger unscaledVal) {
        BigDecimal bd = new BigDecimal(unscaledVal);
        if (bd.compareTo(DECIMAL128_MIN_VALUE) <0 || bd.compareTo(DECIMAL128_MAX_VALUE) > 0) {
            throw new RuntimeException("Decimal128 overflow " + unscaledVal);
        }

        if (unscaledVal.compareTo(BIGINT_MIN_VALUE) < 0) {
            throw new RuntimeException("Decimal128 " + unscaledVal + " cannot be less than " + BIGINT_MIN_VALUE);
        }

        if (unscaledVal.compareTo(BIGINT_MAX_VALUE) > 0) {
            throw new RuntimeException("Decimal128 " + unscaledVal + " cannot exceed " + BIGINT_MAX_VALUE);
        }

        return unscaledVal;
    }

    public BasicDecimal128Vector(DATA_FORM df, ExtendedDataInput in, int extra) throws IOException {
        super(df);
        int rows = in.readInt();
        int cols = in.readInt();
        int size = rows * cols;
        this.unscaledValues = new BigInteger[size];
        if (extra != -1)
            scale_ = extra;
        else
            scale_ = in.readInt();

        byte[] buffer = new byte[4096];
        handleLittleEndianBigEndian(in, this.unscaledValues, buffer, size);

        this.size = this.unscaledValues.length;
        this.capacity = this.unscaledValues.length;
    }

    private static void handleLittleEndianBigEndian(ExtendedDataInput in, BigInteger[] unscaledValues, byte[] buffer, int size) throws IOException {
        int totalBytes = size * 16, off = 0;
        while (off < totalBytes) {
            byte[] val = new byte[16];
            int len = Math.min(4096, totalBytes - off);

            in.readFully(buffer, 0, len);
            if (in.isLittleEndian())
                reverseByteArrayEvery8Byte(buffer);

            int start = off / 16, end = len / 16;
            for (int i = 0; i < end; i++) {
                System.arraycopy(buffer, i * 16, val, 0, 16);
                unscaledValues[i + start] = new BigInteger(val);
            }
            off += len;
        }
    }

    private static BigInteger handleLittleEndianBigEndian(ExtendedDataInput in) throws IOException {
        byte[] buffer = new byte[16];
        BigInteger value;

        if (in.isLittleEndian()) {
            in.readFully(buffer);
            reverseByteArray(buffer);
            value = new BigInteger(buffer);
        } else {
            in.readFully(buffer);
            value = new BigInteger(buffer);
        }

        return value;
    }

    @Override
    public void deserialize(int start, int count, ExtendedDataInput in) throws IOException {
        if (capacity < start + count) {
            BigInteger[] expandedArray = new BigInteger[start + count];
            System.arraycopy(this.unscaledValues, 0, expandedArray, 0, this.unscaledValues.length);
            this.unscaledValues = expandedArray;
			this.size = start + count;
            this.capacity = start + count;
		} else if (this.size < start + count) {
			this.size = start + count;
		}

        for (int i = 0; i < count; i++) {
            this.unscaledValues[start + i] = handleLittleEndianBigEndian(in);
        }
    }

    @Override
    public void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {
        out.writeInt(scale_);
        byte[] newArray = new byte[16 * size];
        for (int i = 0; i < size; i++ ) {
            BigInteger unscaledValue = this.unscaledValues[i];
            if (unscaledValue.compareTo(BIGINT_MIN_VALUE) < 0) {
                throw new RuntimeException("Decimal128 " + unscaledValue + " cannot be less than " + BIGINT_MIN_VALUE);
            }
            if (unscaledValue.compareTo(BIGINT_MAX_VALUE) > 0) {
                throw new RuntimeException("Decimal128 " + unscaledValue + " cannot exceed " + BIGINT_MAX_VALUE);
            }

            byte[] originalArray = unscaledValue.toByteArray();

            if(originalArray.length > 16){
                throw new RuntimeException("byte length of Decimal128 "+originalArray.length+" exceed 16");
            }

            if (originalArray[0] >= 0) {
                // if first bit is 0, represent non-negative.
                System.arraycopy(originalArray, 0, newArray, (16 - originalArray.length) + 16*i, originalArray.length);
            } else {
                // if first bit is 1, represent negative.
                System.arraycopy(originalArray, 0, newArray, (16 - originalArray.length) + 16*i, originalArray.length);
                int newaindex=i*16;
                for (int j = 0; j < 16 - originalArray.length; j++) {
                    newArray[j+newaindex] = -1;
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
        throw new RuntimeException("BasicDecimal128Vector not support combine yet!");
    }

    @Override
    public Vector getSubVector(int[] indices) {
        int length = indices.length;
        BigInteger[] sub = new BigInteger[length];
        for (int i = 0; i < length; i++)
            sub[i] = this.unscaledValues[indices[i]];
        return new BasicDecimal128Vector(sub, scale_);
    }

    @Override
    public int asof(Scalar value) {
        return 0;
    }

    @Override
    public boolean isNull(int index) {
        return this.unscaledValues[index].equals(BIGINT_MIN_VALUE);
    }

    @Override
    public void setNull(int index) {
        this.unscaledValues[index] = BIGINT_MIN_VALUE;
    }

    @Override
    public Entity get(int index) {
        return new BasicDecimal128(this.unscaledValues[index], scale_);
    }

    @Override
    public void set(int index, Entity value) throws Exception {
        if (value.getDataType() != DT_DECIMAL128) {
            throw new RuntimeException("value type is not BasicDecimal128!");
        }

        int newScale = ((Scalar) value).getScale();
        DATA_TYPE type = value.getDataType();
        if (scale_ < 0)
            throw new RuntimeException("scale cannot less than 0.");
        if (((Scalar) value).isNull()) {
            this.unscaledValues[index] = DECIMAL128_MIN_VALUE.toBigInteger();
        } else {
            if (scale_ != newScale) {
                BigInteger newValue;
                if (type == Entity.DATA_TYPE.DT_LONG) {
                    newValue = BigInteger.valueOf(((BasicLong) value).getLong());
                } else if (type == Entity.DATA_TYPE.DT_INT) {
                    newValue = BigInteger.valueOf(((BasicInt) value).getInt());
                } else {
                    newValue = ((BasicDecimal128) value).unscaledValue();
                }

                BigInteger pow = BigInteger.TEN;
                if (newScale - scale_ > 0) {
                    pow = pow.pow(newScale - scale_);
                    newValue = newValue.divide(pow);
                } else {
                    pow = pow.pow(scale_ - newScale);
                    newValue = newValue.multiply(pow);
                }
                this.unscaledValues[index] = newValue;
            } else {
                this.unscaledValues[index] = ((BasicDecimal128) value).unscaledValue();
            }
        }
    }

    @Override
    public Class<?> getElementClass() {
        return BasicDecimal128.class;
    }

    @Override
    public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {
        byte[] buffer = new byte[count * 16];
        for (int i = 0; i < count; i++) {
            BigInteger unscaledValue = this.unscaledValues[start + i];
            byte[] newArray = new byte[16];
            byte[] originalArray = unscaledValue.toByteArray();

            if(originalArray.length == 0 || originalArray.length > 16){
                throw new RuntimeException("byte length of Decimal128 "+originalArray.length+" cannot be less than 0 or exceed 16.");
            }

            if (originalArray[0] >= 0) {
                // if first bit is 0, represent non-negative.
                System.arraycopy(originalArray, 0, newArray, 16 - originalArray.length, originalArray.length);
            } else {
                // if first bit is 1, represent negative.
                System.arraycopy(originalArray, 0, newArray, 16 - originalArray.length, originalArray.length);
                for (int j = 0; j < 16 - originalArray.length; j++) {
                    newArray[j] = -1;
                }
            }

            boolean isLittleEndian = ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);
            if (isLittleEndian) {
                reverseByteArray(newArray);
            }

            System.arraycopy(newArray, 0, buffer, i * 16, 16);
        }
        out.write(buffer);
    }


//    private static void handleLittleEndianBigEndianWrite(ExtendedDataInput in, BigInteger[] unscaledValues, byte[] buffer, int count) throws IOException {
//        int totalBytes = count * 16, off = 0;
//        while (off < totalBytes) {
//            byte[] val = new byte[16];
//            int len = Math.min(4096, totalBytes - off);
//
//            in.readFully(buffer, 0, len);
//            if (in.isLittleEndian())
//                reverseByteArrayEvery8Byte(buffer);
//
//            int start = off / 16, end = len / 16;
//            for (int i = 0; i < end; i++) {
//                System.arraycopy(buffer, i * 16, val, 0, 16);
//                unscaledValues[i + start] = new BigInteger(val);
//            }
//            off += len;
//        }
//    }

    @Override
    public int getUnitLength() {
        return 16;
    }

    public void add(BigDecimal value) {
        if (scale_ < 0) {
            throw new RuntimeException("Please set scale first.");
        }
        if (size + 1 > capacity && this.unscaledValues.length > 0) {
            this.unscaledValues = Arrays.copyOf(this.unscaledValues, this.unscaledValues.length * 2);
        } else if (this.unscaledValues.length == 0) {
            this.unscaledValues = Arrays.copyOf(this.unscaledValues, this.unscaledValues.length + 1);
        }
        capacity = this.unscaledValues.length;
        if (value.compareTo(BigDecimal.ZERO) == 0) {
            this.unscaledValues[size] = BigInteger.ZERO;
        } else {
            this.unscaledValues[size] = value.scaleByPowerOfTen(this.scale_).toBigInteger();
        }
        size++;
    }

    public void add(String value) {
        add(new BigDecimal(value));
    }

    public void addRange(BigInteger[] valueList) {
        int newSize = size + valueList.length;
        if (newSize > capacity && this.unscaledValues.length > 0) {
            this.unscaledValues = Arrays.copyOf(this.unscaledValues, Math.max(this.unscaledValues.length * 2, newSize));
        } else if (this.unscaledValues.length == 0) {
            this.unscaledValues = Arrays.copyOf(this.unscaledValues, valueList.length);
        }

        System.arraycopy(valueList, 0, this.unscaledValues, size, valueList.length);
        size = newSize;
        capacity = this.unscaledValues.length;
    }

    public void addRange(String[] valueList) {
        BigDecimal[] bigDecimalArray = new BigDecimal[valueList.length];
        for (int i = 0; i < valueList.length; i++) {
            bigDecimalArray[i] = new BigDecimal(valueList[i]);
        }

        addRange(bigDecimalArray);
    }

    public void addRange(BigDecimal[] valueList) {
        if (scale_ < 0) {
            throw new RuntimeException("Please set scale first.");
        }

        int newSize = size + valueList.length;
        if (newSize > capacity && this.unscaledValues.length > 0) {
            this.unscaledValues = Arrays.copyOf(this.unscaledValues, Math.max(this.unscaledValues.length * 2, newSize));
        } else if (this.unscaledValues.length == 0) {
            this.unscaledValues = Arrays.copyOf(this.unscaledValues, valueList.length);
        }

        for (int i = 0; i < valueList.length; i ++) {
            if (valueList[i].compareTo(BigDecimal.ZERO) == 0) {
                this.unscaledValues[size + i] = BigInteger.ZERO;
            } else {
                this.unscaledValues[size + i] = valueList[i].scaleByPowerOfTen(this.scale_).toBigInteger();
            }
        }

        size = newSize;
        capacity = this.unscaledValues.length;
    }

    @Override
    public void Append(Scalar value) throws Exception{
        if (scale_ < 0)
            throw new RuntimeException("Please set scale first.");
        add(value.getNumber().toString());
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
            data[i] = this.unscaledValues[i];
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
            byte[] originalArray = this.unscaledValues[indexStart + i].toByteArray();
            if (originalArray.length > unitLength) {
                throw new IOException("BigInteger value exceeds 16 bytes");
            }

            if (originalArray[0] >= 0) {
                // if first bit is 0, represent non-negative.
                System.arraycopy(originalArray, 0, newArray, 16 - originalArray.length, originalArray.length);
            } else {
                // if first bit is 1, represent negative.
                System.arraycopy(originalArray, 0, newArray, 16 - originalArray.length, originalArray.length);
                for (int j = 0; j < 16 - originalArray.length; j++) {
                    newArray[j] = -1;
                }
            }

            boolean isLittleEndian = ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);
            if (isLittleEndian) {
                reverseByteArray(newArray);
            }
            out.put(newArray);
        }
        numElementAndPartial.numElement = targetNumElement;
        numElementAndPartial.partial = 0;
        return targetNumElement * unitLength;
    }
}
