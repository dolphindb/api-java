package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.time.temporal.Temporal;
import java.util.logging.Level;

public class BasicDecimal64 extends AbstractScalar implements Comparable<BasicDecimal32>{
    private int scale_=0;
    private long value_;

    public BasicDecimal64(ExtendedDataInput in) throws IOException{
        scale_ = in.readInt();
        value_ = in.readLong();
    }

    public BasicDecimal64(long value, int scale){
        scale_ = scale;
        value_ = value;
    }

    public BasicDecimal64(double value, int scale){
        scale_ = scale;
        if (value == 0)
            value_ = 0;
        else {
            value_ = (long) Math.floor(value * (long)Math.pow(10, scale_));
        }
    }

    BasicDecimal64(int scale, long value){
        scale_ = scale;
        value_ = value;
    }

    @Override
    protected void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException {
        out.writeInt(scale_);
        out.writeLong(value_);
    }

    @Override
    public DATA_CATEGORY getDataCategory() {
        return DATA_CATEGORY.DECIMAL;
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
            BigDecimal pow = new BigDecimal(10);
            for (long i = 0; i < scale_ - 1; i++) {
                pow = pow.multiply(new BigDecimal(10));
            }
            sb.append(value_ / pow.longValue());
            int sign = value_ < 0 ? -1 : 1;
            BigDecimal result = new BigDecimal(value_ % pow.longValue() * sign);
            sb.append(".");
            String s = result.toString();
            int nowLen = sb.length();
            while (sb.length()-nowLen < scale_ - s.length()){
                sb.append("0");
            }
            sb.append(s);
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
        return value_;
    }

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
    public int compareTo(BasicDecimal32 o) {
        return 0;
    }
}
