package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

public class BasicDecimal128Matrix extends AbstractMatrix {

    private BigInteger[] values;

    private static final BigInteger BIGINT_MIN_VALUE = new BigInteger("-170141183460469231731687303715884105728");
    private static final BigInteger BIGINT_MAX_VALUE = new BigInteger("170141183460469231731687303715884105728");
    private static final BigDecimal DECIMAL128_MIN_VALUE = new BigDecimal("-170141183460469231731687303715884105728");
    private static final BigDecimal DECIMAL128_MAX_VALUE = new BigDecimal("170141183460469231731687303715884105728");


    public BasicDecimal128Matrix(int rows, int columns){
        super(rows, columns);
        this.values = new BigInteger[rows * columns];
    }

    public BasicDecimal128Matrix(int rows, int columns, List<?> listOfArrays, int scale) throws Exception {
        super(rows, columns);

        if (scale < 0 || scale > 38)
            throw new RuntimeException("Scale " + scale + " is out of bounds, it must be in [0,38].");
        this.scale = scale;

        values = new BigInteger[rows * columns];
        if (listOfArrays == null || listOfArrays.size() != columns)
            throw new Exception("input list of arrays does not have " + columns + " columns");
        for (int i = 0; i < columns; ++i) {
            Object array = listOfArrays.get(i);
            if (array instanceof String[]) {
                String[] newArray = (String[]) array;
                BigInteger[] tempArr = new BigInteger[newArray.length];
                if (newArray.length == 0 || newArray.length != rows)
                    throw new Exception("The length of array "+ (i+1) + " doesn't have " + rows + " elements");

                BigDecimal pow = BigDecimal.TEN.pow(this.scale);
                for (int j = 0; j < newArray.length; j ++) {
                    BigDecimal bd = new BigDecimal(newArray[j]);
                    if (bd.compareTo(DECIMAL128_MIN_VALUE) <0 || bd.compareTo(DECIMAL128_MAX_VALUE) > 0)
                        throw new RuntimeException("Decimal128 overflow " + new BigDecimal(newArray[j]).scaleByPowerOfTen(-scale));
                    tempArr[j] = bd.scaleByPowerOfTen(scale).toBigInteger();
                }
                System.arraycopy(tempArr, 0, values, i * rows, rows);
            } else if (array instanceof BigInteger[]) {
                BigInteger[] newArray = (BigInteger[]) listOfArrays.get(i);
                if (newArray.length == 0 || newArray.length != rows)
                    throw new Exception("The length of array "+ (i+1) + " doesn't have " + rows + " elements");
                for (int j = 0; j < newArray.length; j ++)
                    newArray[j] = newArray[j].multiply(BigInteger.TEN.pow(scale));
                System.arraycopy(newArray, 0, values, i * rows, rows);
            } else {
                throw new RuntimeException("BasicDecimal128Matrix 'listOfArrays' param only support String[] or int[].");
            }
        }
    }

    public BasicDecimal128Matrix(ExtendedDataInput in) throws IOException {
        super(in);
    }

    @Override
    public boolean isNull(int row, int column) {
        return values[getIndex(row, column)] == BIGINT_MIN_VALUE;
    }

    @Override
    public void setNull(int row, int column) {
        values[getIndex(row, column)] = BIGINT_MIN_VALUE;
    }

    @Override
    public Scalar get(int row, int column) {
        return new BasicDecimal128(this.scale, values[getIndex(row, column)]);
    }

    public BigInteger getValue(int row, int column) {
        return this.values[getIndex(row, column)];
    }

    public void setValue(int row, int column, BigInteger value) {
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
        return DATA_TYPE.DT_DECIMAL128;
    }

    @Override
    public Class<?> getElementClass(){
        return BasicDecimal128.class;
    }

    @Override
    protected void readMatrixFromInputStream(int rows, int columns,	ExtendedDataInput in)  throws IOException{
        int size = rows * columns;
        values =new BigInteger[size];
        long totalBytes = (long)size * 16, off = 0;
        this.scale = in.readInt();
        while (off < totalBytes) {
            byte[] val = new byte[16];
            int len = (int)Math.min(BUF_SIZE, totalBytes - off);
            in.readFully(buf, 0, len);
            if (in.isLittleEndian())
                reverseByteArrayEvery8Byte(buf);

            int start = (int)(off / 16), end = len / 16;
            for (int i = 0; i < end; i++) {
                System.arraycopy(buf, i * 16, val, 0, 16);
                values[i + start] = new BigInteger(val);
            }
            off += len;
        }
    }

    public static void reverseByteArrayEvery8Byte(byte[] array) {
        int start = 0;
        int end = start + 15;

        while (end < array.length) {
            for (int i = 0; i < 8; i++) {
                byte temp = array[start + i];
                array[start + i] = array[end - i];
                array[end - i] = temp;
            }

            start += 16;
            end += 16;
        }
    }

    protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
        out.writeInt(this.scale);
        byte[] newArray = new byte[16 * this.values.length];
        for (int i = 0; i < this.values.length; i++ ) {
            BigInteger unscaledValue = this.values[i];
            if (unscaledValue.compareTo(BIGINT_MIN_VALUE) < 0) {
                throw new RuntimeException("Decimal128 " + unscaledValue + " cannot be less than " + BIGINT_MIN_VALUE);
            }
            if (unscaledValue.compareTo(BIGINT_MAX_VALUE) > 0) {
                throw new RuntimeException("Decimal128 " + unscaledValue + " cannot exceed " + BIGINT_MAX_VALUE);
            }

            byte[] originalArray = unscaledValue.toByteArray();

            if(originalArray.length > 16){
                throw new RuntimeException("byte length of Decimal128 "+originalArray.length+" exceed 16");
            }

            if (originalArray[0] >= 0) {
                // if first bit is 0, represent non-negative.
                System.arraycopy(originalArray, 0, newArray, (16 - originalArray.length) + 16*i, originalArray.length);
            } else {
                // if first bit is 1, represent negative.
                System.arraycopy(originalArray, 0, newArray, (16 - originalArray.length) + 16*i, originalArray.length);
                int newaindex=i*16;
                for (int j = 0; j < 16 - originalArray.length; j++) {
                    newArray[j+newaindex] = -1;
                }
            }
        }

        reverseByteArrayEvery8Byte(newArray);
        out.write(newArray);
    }
}
