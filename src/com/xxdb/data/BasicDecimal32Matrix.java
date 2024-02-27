package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class BasicDecimal32Matrix extends AbstractMatrix {
    private int[] values;

    public BasicDecimal32Matrix(int rows, int columns){
        super(rows, columns);
        this.values = new int[rows * columns];
    }

    public BasicDecimal32Matrix(int rows, int columns, List<int[]> listOfArrays) throws Exception {
        super(rows, columns);
        values = new int[rows * columns];
        if (listOfArrays == null || listOfArrays.size() != columns)
            throw new Exception("input list of arrays does not have " + columns + " columns");
        for (int i=0; i<columns; ++i) {
            int[] array = listOfArrays.get(i);
            if (array == null || array.length != rows)
                throw new Exception("The length of array "+ (i+1) + " doesn't have " + rows + " elements");
            System.arraycopy(array, 0, values, i * rows, rows);
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
        values[getIndex(row, column)] = -Integer.MAX_VALUE;
    }

    @Override
    public Scalar get(int row, int column) {
        return new BasicDecimal32(new int[]{this.scale, values[getIndex(row, column)]});
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