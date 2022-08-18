package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

import java.io.IOException;
import java.time.temporal.Temporal;

public class BasicDecimal64 extends AbstractScalar implements Comparable<BasicDecimal32>{
    private int scale_=0;
    private long value_;

    public BasicDecimal64(ExtendedDataInput in) throws IOException{
        scale_ = in.readInt();
        value_ = in.readLong();
    }

    public BasicDecimal64(int scale, long value){
        scale_ = scale;
        value_ = value;
    }

    @Override
    protected void writeScalarToOutputStream(ExtendedDataOutput out) throws IOException {

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
        if (scale_ == 0)
            return String.valueOf(value_);
        else {
            StringBuilder sb = new StringBuilder();
            sb.append((long)(value_ / Math.pow(10, scale_)));
            int sign = value_ < 0? -1 : 1;
            double data = value_ % Math.pow(10, scale_) * sign;
            sb.append(".");
            if (data == 0){
                for (int i = 0; i < scale_;i++){
                    sb.append("0");
                }
            }else {
                sb.append(data);
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
