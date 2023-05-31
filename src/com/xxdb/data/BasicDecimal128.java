package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.temporal.Temporal;
import java.util.Objects;

public class BasicDecimal128 extends AbstractScalar implements Comparable<BasicDecimal128> {
    private int scale_;
    private BigInteger value_;

    private static final BigDecimal DECIMAL128_MIN_VALUE = new BigDecimal("-170141183460469231731687303715884105728");
    private static final BigDecimal DECIMAL128_MAX_VALUE = new BigDecimal("170141183460469231731687303715884105728");
    public static final BigInteger BIGINT_MIN_VALUE = new BigInteger("-170141183460469231731687303715884105728");
    public static final BigInteger BIGINT_MAX_VALUE = new BigInteger("170141183460469231731687303715884105728");

    public BasicDecimal128(ExtendedDataInput in) throws IOException {
        scale_ = in.readInt();
        value_ = handleLittleEndianBigEndian(in);
    }

    public BasicDecimal128(String data, int scale){
        BigDecimal bd = new BigDecimal(data);
        if(bd.compareTo(DECIMAL128_MIN_VALUE)<0||bd.compareTo(DECIMAL128_MAX_VALUE)>0){
            throw new RuntimeException("Decimal128 overflow "+data);
        }
        value_ = bd.scaleByPowerOfTen(scale).toBigInteger();
        while(value_.compareTo(BIGINT_MIN_VALUE)<0){
            value_=value_.divide(BigInteger.valueOf(10));
        }
        while(value_.compareTo(BIGINT_MAX_VALUE)>0){
            value_=value_.divide(BigInteger.valueOf(10));
        }
        scale_ = scale;
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

    public BasicDecimal128(BigInteger value, int scale) {
        scale_ = scale;
        if (Objects.isNull(value)) {
            value_ = DECIMAL128_MIN_VALUE.toBigInteger();
        } else {
            BigDecimal bd = new BigDecimal(value);
            value_ = bd.scaleByPowerOfTen(scale).toBigInteger();
        }
    }

    public BasicDecimal128(int scale, BigInteger value) {
        scale_ = scale;
        value_ = value;
    }

    public BasicDecimal128(double value, int scale) {
        scale_ = scale;
        BigDecimal bd = new BigDecimal(Double.toString(value));
        value_ = bd.scaleByPowerOfTen(scale).toBigInteger();
    }

    @Override
    protected void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException {
        out.writeInt(scale_);

        byte[] newArray = new byte[16];
        byte[] originalArray = value_.toByteArray();

        if (((originalArray[0] >> 7) & 1) == 0) {
            // if first bit is 0, represent non-negative.
            System.arraycopy(originalArray, 0, newArray, 16 - originalArray.length, originalArray.length);
        } else {
            // if first bit is 1, represent negative.
            System.arraycopy(originalArray, 0, newArray, 16 - originalArray.length, originalArray.length);
            for (int i = 0; i < 16 - originalArray.length; i++) {
                newArray[i] = -1;
            }
        }

        reverseByteArray(newArray);
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

    @Override
    public DATA_CATEGORY getDataCategory() {
        return DATA_CATEGORY.DENARY;
    }

    @Override
    public DATA_TYPE getDataType() {
        return DATA_TYPE.DT_DECIMAL128;
    }

    @Override
    public String getString() {
        if (scale_ == 0 && !isNull()) {
            return value_.toString();
        } else if (isNull()) {
            return "";
        } else {
            BigDecimal bd = new BigDecimal(value_).scaleByPowerOfTen(-scale_);
            return bd.toPlainString();
        }
    }

    @Override
    public boolean isNull() {
        if (Objects.isNull(value_)) {
            return true;
        }
        return new BigDecimal(value_).compareTo(DECIMAL128_MIN_VALUE) == 0;
    }

    @Override
    public void setNull() {
        value_ = DECIMAL128_MIN_VALUE.toBigInteger();
    }

    @Override
    public Number getNumber() throws Exception {
        if (isNull()) {
            return DECIMAL128_MIN_VALUE.toBigInteger();
        } else {
            BigDecimal bd = new BigDecimal(value_).scaleByPowerOfTen(-scale_).stripTrailingZeros();
            if (bd.doubleValue() % 1 == 0) {
                return bd.longValue();
            } else {
                return bd.doubleValue();
            }
        }
    }

    @Override
    public int getScale() {
        return scale_;
    }

    @Override
    public Temporal getTemporal() throws Exception {
        throw new Exception("Incompatible data type");
    }

    @Override
    public int hashBucket(int buckets) {
        return 0;
    }

    @Override
    public String getJsonString() {
        if (isNull()) {
            return "null";
        } else {
            return getString();
        }
    }

    @Override
    public int compareTo(BasicDecimal128 o) {
        BigDecimal a = new BigDecimal(getString());
        BigDecimal b = new BigDecimal(o.getString());
        return a.compareTo(b);
    }

    public BigInteger getBigInteger() {
        return value_;
    }
}
