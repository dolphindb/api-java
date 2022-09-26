package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.temporal.Temporal;

public class BasicDecimal32 extends AbstractScalar implements Comparable<BasicDecimal32>{
    private int scale_=0;
    private int value_;

    public BasicDecimal32(ExtendedDataInput in) throws IOException{
        scale_ = in.readInt();
        value_ = in.readInt();
    }

    public BasicDecimal32(int value, int scale){
        scale_ = scale;
        value_ = value * (int)Math.pow(10, scale_);
    }

    public BasicDecimal32(double value, int scale){
        scale_ = scale;
        if (value == 0)
            value_ = 0;
        else {
            value_ = (int) Math.floor(value * (int)Math.pow(10, scale_));
        }
    }

    BasicDecimal32(int[] all){
        scale_ = all[0];
        value_ = all[1];
    }

    @Override
    protected void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException {
        out.writeInt(scale_);
        out.writeInt(value_);
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
            if (value_ < 0 && (value_ / pow.longValue()) == 0)
                sb.append("-");
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
        return value_ == Integer.MIN_VALUE;
    }

    @Override
    public void setNull() {
        value_ = Integer.MIN_VALUE;
    }

    @Override
    public Number getNumber() throws Exception {
        if (isNull())
            return Integer.MIN_VALUE;
        else
            return value_;
    }

    @Override
    public int getScale(){
        return scale_;
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
        double a = Double.parseDouble(getString());
        double b = Double.parseDouble(o.getString());
        return Double.compare(a, b);
    }
}
