package com.xxdb.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import static com.xxdb.data.Entity.DATA_TYPE.DT_DECIMAL64;

public class BasicDecimal64Matrix extends AbstractMatrix {
    private long[] values;

    private static final long MIN_VALUE = Long.MIN_VALUE;
    private static final long MAX_VALUE = Long.MAX_VALUE;

    public BasicDecimal64Matrix(int rows, int columns, int scale){
        super(rows, columns, scale);

        if (scale < 0 || scale > 18)
            throw new RuntimeException("Scale " + scale + " is out of bounds, it must be in [0,18].");

        this.values = new long[rows * columns];
    }

    public BasicDecimal64Matrix(int rows, int columns, List<?> listOfArrays, int scale) throws Exception {
        super(rows, columns);

        if (scale < 0 || scale > 18)
            throw new RuntimeException("Scale " + scale + " is out of bounds, it must be in [0,18].");
        this.scale = scale;

        values = new long[rows * columns];
        if (listOfArrays == null || listOfArrays.size() != columns)
            throw new Exception("input list of arrays does not have " + columns + " columns");
        for (int i = 0; i < columns; ++i) {
            Object array = listOfArrays.get(i);
            if (array instanceof String[]) {
                String[] newArray = (String[]) array;
                long[] tempArr = new long[newArray.length];
                if (newArray.length == 0 || newArray.length != rows)
                    throw new Exception("The length of array "+ (i+1) + " doesn't have " + rows + " elements");

                for (int j = 0; j < newArray.length; j ++) {
                    BigDecimal bd = new BigDecimal(newArray[j]);
                    BigDecimal multipliedValue = bd.scaleByPowerOfTen(scale).setScale(0, RoundingMode.HALF_UP);
                    if (multipliedValue.longValue() > MIN_VALUE && multipliedValue.longValue() < MAX_VALUE)
                        tempArr[j] = multipliedValue.longValue();
                }
                System.arraycopy(tempArr, 0, values, i * rows, rows);
            } else if (array instanceof long[]) {
                long[] newArray = (long[]) listOfArrays.get(i);
                if (newArray.length == 0 || newArray.length != rows)
                    throw new Exception("The length of array "+ (i+1) + " doesn't have " + rows + " elements");
                for (int j = 0; j < newArray.length; j ++)
                    newArray[j] = newArray[j] * (long) Math.pow(10, this.scale);
                System.arraycopy(newArray, 0, values, i * rows, rows);
            } else {
                throw new RuntimeException("BasicDecimal64Matrix 'listOfArrays' param only support String[] or long[].");
            }
        }
    }

    public BasicDecimal64Matrix(ExtendedDataInput in) throws IOException {
        super(in);
    }

    @Override
    public boolean isNull(int row, int column) {
        return values[getIndex(row, column)] == MIN_VALUE;
    }

    @Override
    public void setNull(int row, int column) {
        values[getIndex(row, column)] = MIN_VALUE;
    }

    @Override
    public Scalar get(int row, int column) {
        return new BasicDecimal64(this.scale, values[getIndex(row, column)]);
    }

    public void set(int row, int column, Entity value) {
        if (!value.getDataForm().equals(DATA_FORM.DF_SCALAR) || value.getDataType() != DT_DECIMAL64) {
            throw new RuntimeException("The value type is not BasicDecimal64!");
        }

        int newScale = ((Scalar) value).getScale();
        DATA_TYPE type = value.getDataType();
        if (this.scale < 0)
            this.scale = newScale;
        if (((Scalar)value).isNull())
            this.values[getIndex(row, column)] = MIN_VALUE;
        else {
            if(this.scale != newScale) {
                BigInteger newValue;
                if (type == Entity.DATA_TYPE.DT_LONG) {
                    newValue = BigInteger.valueOf(((BasicLong)(value)).getLong());
                } else if (type == Entity.DATA_TYPE.DT_INT) {
                    newValue = BigInteger.valueOf(((BasicInt)(value)).getInt());
                } else {
                    newValue = BigInteger.valueOf(((BasicDecimal64) (value)).getLong());
                }

                BigInteger pow = BigInteger.valueOf(10);
                if (newScale - this.scale > 0) {
                    pow = pow.pow(newScale - this.scale);
                    newValue = newValue.divide(pow);
                } else{
                    pow = pow.pow(this.scale - newScale);
                    newValue = newValue.multiply(pow);
                }
                this.values[getIndex(row, column)] = newValue.longValue();
            } else {
                this.values[getIndex(row, column)] = ((BasicDecimal64) value).getLong();
            }
        }
    }

    public void setScale(int scale) {
        this.scale = scale;
    }

    @JsonIgnore
    @Override
    public int getScale() {
        return this.scale;
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
    public Class<?> getElementClass(){
        return BasicDecimal64.class;
    }

    @Override
    protected void readMatrixFromInputStream(int rows, int columns,	ExtendedDataInput in)  throws IOException{
        int size = rows * columns;
        values =new long[size];
        long totalBytes = (long)size * 8, off = 0;
        ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
        this.scale = in.readInt();
        while (off < totalBytes) {
            int len = (int)Math.min(BUF_SIZE, totalBytes - off);
            in.readFully(buf, 0, len);
            int start = (int)(off / 8), end = len / 8;
            ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
            for (int i = 0; i < end; i++)
                values[i + start] = byteBuffer.getLong(i * 8);
            off += len;
        }
    }

    protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
        out.writeInt(this.scale);
        out.writeLongArray(this.values);
    }


}
