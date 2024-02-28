package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class BasicDecimal64Matrix extends AbstractMatrix {
    private long[] values;

    public BasicDecimal64Matrix(int rows, int columns){
        super(rows, columns);
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

                BigDecimal pow = BigDecimal.TEN.pow(this.scale);
                for (int j = 0; j < newArray.length; j ++) {
                    BigDecimal bd = new BigDecimal(newArray[j]);
                    if (bd.multiply(pow).intValue() > Integer.MIN_VALUE && bd.multiply(pow).intValue() < Integer.MAX_VALUE)
                        tempArr[j] = bd.multiply(pow).intValue();
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
                throw new RuntimeException("BasicDecimal64Matrix 'listOfArrays' param only support String[] or int[].");
            }
        }
    }

    public BasicDecimal64Matrix(ExtendedDataInput in) throws IOException {
        super(in);
    }

    @Override
    public boolean isNull(int row, int column) {
        return values[getIndex(row, column)] == -Long.MAX_VALUE;
    }

    @Override
    public void setNull(int row, int column) {
        values[getIndex(row, column)] = -Long.MAX_VALUE;
    }

    @Override
    public Scalar get(int row, int column) {
        return new BasicDecimal64(this.scale, values[getIndex(row, column)]);
    }

    public long getValue(int row, int column) {
        return this.values[getIndex(row, column)];
    }

    public void setValue(int row, int column, long value) {
        this.values[getIndex(row, column)] = value;
    }

    public void setScale(int scale) {
        this.scale = scale;
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
                values[i + start] = byteBuffer.getInt(i * 8);
            off += len;
        }
    }

    protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
        out.writeInt(this.scale);
        out.writeLongArray(this.values);
    }


}
