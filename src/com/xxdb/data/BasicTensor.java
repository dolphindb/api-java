package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import java.io.IOException;

public class BasicTensor extends AbstractTensor {

    private DATA_TYPE dataType;
    private int tensorType;
    private int deviceType;
    private int tensorFlags;
    private int dimensions;

    /**
     * shapes: shape[i] represents the size of the i-th dimension.
     */
    private long[] shapes;

    /**
     * strides: strides[i] represents the distance between elements in the i-th dimension.
     */
    private long[] strides;

    private long preserveValue;

    private long elemCount;

    private Vector data;

    protected BasicTensor(DATA_TYPE dataType, ExtendedDataInput in) throws IOException {
        deserialize(dataType, in);
    }

    protected void deserialize(DATA_TYPE dataType, ExtendedDataInput in) throws IOException {
        this.dataType = dataType;
        tensorType = in.readByte();
        deviceType = in.readByte();
        tensorFlags = in.readInt();
        dimensions = in.readInt();

        shapes = new long[dimensions];
        strides = new long[dimensions];

        for (int d = 0; d < dimensions; d++)
            shapes[d] = in.readLong();

        for (int d = 0; d < dimensions; d++)
            strides[d] = in.readLong();

        preserveValue = in.readLong();
        elemCount = in.readLong();

        if (elemCount > Integer.MAX_VALUE)
            throw new RuntimeException("tensor element count more than 2,147,483,647(Integer.MAX_VALUE).");

        Vector subVector = BasicEntityFactory.instance().createVectorWithDefaultValue(dataType, (int) elemCount, -1);
        subVector.deserialize(0, (int) elemCount, in);
        this.data = subVector;
    }

    @Override
    public DATA_CATEGORY getDataCategory() {
        return getDataCategory(dataType);
    }

    @Override
    public DATA_TYPE getDataType() {
        return dataType;
    }

    @Override
    public int rows() {
        return data.rows();
    }

    @Override
    public void write(ExtendedDataOutput output) throws IOException {
        throw new RuntimeException("BasicTensor not support write method.");
    }

    public int getDimensions() {
        return dimensions;
    }

    public long[] getShapes() {
        return shapes;
    }

    public long[] getStrides() {
        return strides;
    }

    public long getElemCount() {
        return elemCount;
    }

    public Vector getData() {
        return data;
    }

    @Override
    public String getString() {
        StringBuilder sb = new StringBuilder();
        sb.append("tensor<").append(getDataTypeString());;
        for (long shape : shapes) {
            sb.append("[").append(shape).append("]");
        }
        sb.append(">(");
        printTensor(sb, 0, 0, new int[dimensions]);
        sb.append(")");
        return sb.toString();
    }

    private void printTensor(StringBuilder sb, int depth, int index, int[] indices) {
        if (depth == dimensions) {
            int flatIndex = getFlatIndex(indices);
            sb.append(data.get(flatIndex));
            return;
        }

        sb.append("[");
        long size = shapes[depth];
        for (int i = 0; i < size; i++) {
            indices[depth] = i;
            if (depth == dimensions - 1 && size > 11 && i == 11) {
                sb.append("...");
                break;
            } else {
                if (i > 0) {
                    sb.append(",");
                }
                printTensor(sb, depth + 1, index * (int) size + i, indices);
            }
        }
        sb.append("]");
    }

    private String getDataTypeString() {
        switch (dataType) {
            case DT_BOOL:
                return "bool";
            case DT_BYTE:
                return "char";
            case DT_SHORT:
                return "short";
            case DT_INT:
                return "int";
            case DT_LONG:
                return "long";
            case DT_FLOAT:
                return "float";
            case DT_DOUBLE:
                return "double";
            default:
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }

    private int getFlatIndex(int[] indices) {
        int flatIndex = 0;
        int stride = 1;
        for (int i = dimensions - 1; i >= 0; i--) {
            flatIndex += indices[i] * stride;
            stride *= shapes[i];
        }
        return flatIndex;
    }
}
