package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

import java.io.IOException;
import java.time.temporal.Temporal;

public class BasicDecimal32 extends AbstractScalar implements Comparable<BasicDecimal32>{
    private int scale_=0;
    private int value_;

    public BasicDecimal32(ExtendedDataInput in) throws IOException{
        scale_ = in.readInt();
        value_ = in.readInt();
    }

    public BasicDecimal32(int scale, int value){
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
            sb.append((int)(value_ / Math.pow(10, scale_)));
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
        return value_ == Integer.MIN_VALUE;
    }

    @Override
    public void setNull() {
        value_ = Integer.MIN_VALUE;
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
