package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.temporal.Temporal;

public class BasicDecimal128 extends AbstractScalar implements Comparable<BasicDecimal128> {
    private int scale_;
    private BigInteger value_;

    public BasicDecimal128(ExtendedDataInput in) throws IOException {
        scale_ = in.readInt();

        byte[] buffer = new byte[16];
        for (int i = buffer.length-1; i >=0; i--) {
            buffer[i] = in.readByte();
        }
        value_ = new BigInteger(buffer);
    }

    public BasicDecimal128(BigInteger value, int scale) {
        scale_ = scale;
        value_ = new BigInteger(value.toByteArray());
    }

    public BasicDecimal128(double value, int scale) {
        scale_ = scale;
        BigDecimal bd = new BigDecimal(value);
        value_ = bd.scaleByPowerOfTen(scale).toBigInteger();
    }

    @Override
    protected void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException {
        out.writeInt(scale_);
        byte[] buffer = value_.toByteArray();
        out.writeInt(buffer.length);
        out.write(buffer);
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
        return value_ == null;
    }

    @Override
    public void setNull() {
        value_ = null;
    }

    @Override
    public Number getNumber() throws Exception {
        if (isNull()) {
            return null;
        } else {
            BigDecimal bd = new BigDecimal(value_).scaleByPowerOfTen(-scale_);
            return bd.doubleValue();
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
