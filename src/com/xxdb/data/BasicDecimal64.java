package com.xxdb.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.temporal.Temporal;


public class BasicDecimal64 extends AbstractScalar implements Comparable<BasicDecimal64>{
    private int scale_=0;
    private long value_;

    private static final BigDecimal DECIMAL64_MIN_VALUE = new BigDecimal("-9223372036854775808");
    private static final BigDecimal DECIMAL64_MAX_VALUE = new BigDecimal("9223372036854775807");

    public BasicDecimal64(ExtendedDataInput in) throws IOException{
        scale_ = in.readInt();
        value_ = in.readLong();
    }

    public BasicDecimal64(long value, int scale){
        this(String.valueOf(value), scale);
    }

    public BasicDecimal64(String value, int scale) {
        if (Utils.isEmpty(value)) {
            throw new RuntimeException("value is empty!");
        }

        if (scale < 0 || scale > 18) {
            throw new RuntimeException("Scale " + scale + " is out of bounds, it must be in [0,18].");
        }
        scale_ = scale;

        if ("0".equals(value)) {
            value_ = 0L;
        } else {
            BigInteger unscaledVal = new BigDecimal(value).scaleByPowerOfTen(scale).setScale(0, RoundingMode.HALF_UP).toBigInteger();
            BigDecimal bd = new BigDecimal(unscaledVal);
            if (bd.compareTo(DECIMAL64_MIN_VALUE) < 0  || bd.compareTo(DECIMAL64_MAX_VALUE) > 0) {
                throw new RuntimeException("Decimal math overflow: " + value);
            } else {
                value_ = bd.longValue();
            }
        }
    }

    @Deprecated
    public BasicDecimal64(double value, int scale){
        scale_ = scale;
        if (value == 0)
            value_ = 0;
        else {
            BigDecimal pow = new BigDecimal(1);
            for (long i = 0; i < scale_; i++) {
                pow = pow.multiply(new BigDecimal(10));
            }
            BigDecimal dbvalue = new BigDecimal(Double.toString(value));
            value_ = (dbvalue.multiply(pow)).longValue();
        }
    }

    BasicDecimal64(int scale, long value){
        scale_ = scale;
        value_ = value;
    }

    @Override
    public void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException {
        out.writeInt(scale_);
        out.writeLong(value_);
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
    public String getString() {
        if (scale_ == 0&&(!isNull()))
            return String.valueOf(value_);
        else if (isNull())
            return "";
        else {
            StringBuilder sb = new StringBuilder();
            BigDecimal pow = new BigDecimal(1);
            for (long i = 0; i < scale_; i++) {
                pow = pow.multiply(new BigDecimal(10));
            }
            if (value_ < 0 && (value_ / pow.longValue()) == 0)
                sb.append("-");
            sb.append(value_ / pow.longValue());
            if (pow.intValue() != 1) {
                int sign = value_ < 0 ? -1 : 1;
                BigDecimal result = new BigDecimal(value_ % pow.longValue() * sign);
                sb.append(".");
                String s = result.toString();
                int nowLen = sb.length();
                while (sb.length() - nowLen < scale_ - s.length()) {
                    sb.append("0");
                }
                sb.append(s);
            }
            return sb.toString();
        }
    }

    @Override
    public boolean isNull() {
        return value_ == Long.MIN_VALUE;
    }

    @Override
    public void setNull() {
        value_ = Long.MIN_VALUE;
    }

    @Override
    public Number getNumber() throws Exception {
        if (isNull())
            return Long.MIN_VALUE;
        else{
            BigDecimal pow = new BigDecimal(1);
            for (long i = 0; i < scale_; i++) {
                pow = pow.multiply(new BigDecimal(10));
            }
            BigDecimal dbvalue = new BigDecimal(value_);
            double num = (dbvalue.divide(pow)).doubleValue();
            if (num % 1 == 0)
                return (long)num;
            else
                return num;
        }
    }

    @Override
    public int getScale(){
        return scale_;
    }

    @JsonIgnore
    @Override
    public Temporal getTemporal() throws Exception {
        throw new Exception("Imcompatible data type");
    }

    @Override
    public int hashBucket(int buckets) {
        return 0;
    }

    @Override
    public String getJsonString() {
        if(isNull()) return "null";
        return getString();
    }

    @Override
    public int compareTo(BasicDecimal64 o) {
        double a = Double.parseDouble(getString());
        double b = Double.parseDouble(o.getString());
        return Double.compare(a, b);
    }

    public long getLong(){
        return value_;
    }
}
