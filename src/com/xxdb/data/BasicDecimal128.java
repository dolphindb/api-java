package com.xxdb.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.temporal.Temporal;
import java.util.Objects;

public class BasicDecimal128 extends AbstractScalar implements Comparable<BasicDecimal128> {
    private int scale;
    private BigInteger unscaledValue;

    private static final BigDecimal DECIMAL128_MIN_VALUE = new BigDecimal("-170141183460469231731687303715884105728");
    private static final BigDecimal DECIMAL128_MAX_VALUE = new BigDecimal("170141183460469231731687303715884105728");
    private static final BigInteger BIGINT_MIN_VALUE = new BigInteger("-170141183460469231731687303715884105728");
    private static final BigInteger BIGINT_MAX_VALUE = new BigInteger("170141183460469231731687303715884105728");

    public BasicDecimal128(String data, int scale) {
        this(new BigDecimal(data).scaleByPowerOfTen(scale).toBigInteger(), scale);
    }

    public BasicDecimal128(BigInteger unscaledVal, int scale) {
        if (scale < 0 || scale > 38) {
            throw new RuntimeException("Scale out of bound (valid range: [0, 38], but get: " + scale + ")");
        }

        BigDecimal bd = new BigDecimal(unscaledVal);
        if (bd.compareTo(DECIMAL128_MIN_VALUE) <0 || bd.compareTo(DECIMAL128_MAX_VALUE) > 0) {
            throw new RuntimeException("Decimal128 overflow " + unscaledVal);
        }

        unscaledValue = unscaledVal;
        if (unscaledValue.compareTo(BIGINT_MIN_VALUE) < 0) {
            throw new RuntimeException("Decimal128 " + unscaledValue + " cannot be less than " + BIGINT_MIN_VALUE);
        }

        if (unscaledValue.compareTo(BIGINT_MAX_VALUE) > 0) {
            throw new RuntimeException("Decimal128 " + unscaledValue + " cannot exceed " + BIGINT_MAX_VALUE);
        }

        this.scale = scale;
    }

    public BasicDecimal128(ExtendedDataInput in) throws IOException {
        scale = in.readInt();
        unscaledValue = handleLittleEndianBigEndian(in);
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
    protected void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException {
        out.writeInt(this.scale);

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
            for (int i = 0; i < 16 - originalArray.length; i++) {
                newArray[i] = -1;
            }
        }

        out.writeBigIntArray(newArray, 0, newArray.length);
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
        if (isNull()) {
            return "";
        } else if (this.scale == 0) {
            return unscaledValue.toString();
        } else {
            BigDecimal bd = new BigDecimal(unscaledValue).scaleByPowerOfTen(-this.scale);
            return bd.toPlainString();
        }
    }

    @Override
    public boolean isNull() {
        if (Objects.isNull(unscaledValue)) {
            return true;
        }
        return new BigDecimal(unscaledValue).compareTo(DECIMAL128_MIN_VALUE) == 0;
    }

    @Override
    public void setNull() {
        unscaledValue = DECIMAL128_MIN_VALUE.toBigInteger();
    }

    @Override
    public Number getNumber() throws Exception {
        if (isNull()) {
            return DECIMAL128_MIN_VALUE;
        } else {
            BigDecimal bd = new BigDecimal(unscaledValue).scaleByPowerOfTen(-this.scale).stripTrailingZeros();
            return bd;
        }
    }

    @Override
    public int getScale() {
        return this.scale;
    }

    @JsonIgnore
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

    public BigInteger unscaledValue() {
        return unscaledValue;
    }
}
