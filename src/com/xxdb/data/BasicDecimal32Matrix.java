package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import static com.xxdb.data.Entity.DATA_TYPE.DT_DECIMAL32;

public class BasicDecimal32Matrix extends AbstractMatrix {
    private int[] values;

    public BasicDecimal32Matrix(int rows, int columns){
        super(rows, columns);
        this.values = new int[rows * columns];
    }

    public BasicDecimal32Matrix(int rows, int columns, List<?> listOfArrays, int scale) throws Exception {
        super(rows, columns);

        if (scale < 0 || scale > 9)
            throw new RuntimeException("Scale " + scale + " is out of bounds, it must be in [0,9].");
        this.scale = scale;

        values = new int[rows * columns];
        if (listOfArrays == null || listOfArrays.size() != columns)
            throw new Exception("input list of arrays does not have " + columns + " columns");
        for (int i = 0; i < columns; ++i) {
            Object array = listOfArrays.get(i);
            if (array instanceof String[]) {
                String[] newArray = (String[]) array;
                int[] tempArr = new int[newArray.length];
                if (newArray.length == 0 || newArray.length != rows)
                    throw new Exception("The length of array "+ (i+1) + " doesn't have " + rows + " elements");

                BigDecimal pow = BigDecimal.TEN.pow(this.scale);
                for (int j = 0; j < newArray.length; j ++) {
                    BigDecimal bd = new BigDecimal(newArray[j]);
                    if (bd.multiply(pow).intValue() > Integer.MIN_VALUE && bd.multiply(pow).intValue() < Integer.MAX_VALUE)
                        tempArr[j] = bd.multiply(pow).intValue();
                }
                System.arraycopy(tempArr, 0, values, i * rows, rows);
            } else if (array instanceof int[]) {
                int[] newArray = (int[]) listOfArrays.get(i);
                if (newArray.length == 0 || newArray.length != rows)
                    throw new Exception("The length of array "+ (i+1) + " doesn't have " + rows + " elements");
                for (int j = 0; j < newArray.length; j ++)
                    newArray[j] = newArray[j] * (int)Math.pow(10, this.scale);
                System.arraycopy(newArray, 0, values, i * rows, rows);
            } else {
                throw new RuntimeException("BasicDecimal32Matrix 'listOfArrays' param only support String[] or int[].");
            }
        }
    }

    public BasicDecimal32Matrix(ExtendedDataInput in) throws IOException {
        super(in);
    }

    @Override
    public boolean isNull(int row, int column) {
        return values[getIndex(row, column)] == -Integer.MAX_VALUE;
    }

    @Override
    public void setNull(int row, int column) {
        this.values[getIndex(row, column)] = -Integer.MAX_VALUE;
    }

    @Override
    public Scalar get(int row, int column) {
        return new BasicDecimal32(new int[]{this.scale, this.values[getIndex(row, column)]});
    }

    public void set(int row, int column, Entity value) {
        if (!value.getDataForm().equals(DATA_FORM.DF_SCALAR) || value.getDataType() != DT_DECIMAL32)
            throw new RuntimeException("The value type is not BasicDecimal32!");

        int newScale = ((Scalar) value).getScale();
        if (this.scale < 0)
            this.scale = newScale;
        if (((Scalar)value).isNull())
            this.values[getIndex(row, column)] = -Integer.MAX_VALUE;
        else {
            if(this.scale != newScale) {
                BigInteger newValue = BigInteger.valueOf(((BasicDecimal32) (value)).getInt());
                BigInteger pow = BigInteger.valueOf(10);
                if (newScale - this.scale > 0) {
                    pow = pow.pow(newScale - this.scale);
                    newValue = newValue.divide(pow);
                } else{
                    pow = pow.pow(this.scale - newScale);
                    newValue = newValue.multiply(pow);
                }
                this.values[getIndex(row, column)] = newValue.intValue();
            } else {
                this.values[getIndex(row, column)] = ((BasicDecimal32) value).getInt();
            }
        }
    }

    public int getValue(int row, int column) {
        return this.values[getIndex(row, column)];
    }

    public void setScale(int scale) {
        this.scale = scale;
    }

    public int getScale() {
        return this.scale;
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
    public Class<?> getElementClass(){
        return BasicDecimal32.class;
    }

    @Override
    protected void readMatrixFromInputStream(int rows, int columns,	ExtendedDataInput in)  throws IOException{
        int size = rows * columns;
        values =new int[size];
        long totalBytes = (long)size * 4, off = 0;
        ByteOrder bo = in.isLittleEndian() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
        this.scale = in.readInt();
        while (off < totalBytes) {
            int len = (int)Math.min(BUF_SIZE, totalBytes - off);
            in.readFully(buf, 0, len);
            int start = (int)(off / 4), end = len / 4;
            ByteBuffer byteBuffer = ByteBuffer.wrap(buf, 0, len).order(bo);
            for (int i = 0; i < end; i++)
                values[i + start] = byteBuffer.getInt(i * 4);
            off += len;
        }
    }

    protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
        out.writeInt(this.scale);
        out.writeIntArray(this.values);
    }

}