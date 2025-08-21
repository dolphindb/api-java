package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

public class BasicVoidVector extends AbstractVector {

    private int size;

    public BasicVoidVector(int size){
        this(Entity.DATA_FORM.DF_VECTOR, size);
    }

    protected BasicVoidVector(Entity.DATA_FORM df, int size){
        super(df);
        this.size = size;
    }

    protected BasicVoidVector(Entity.DATA_FORM df, ExtendedDataInput in) throws IOException {
        super(df);
        int rows = in.readInt();
        int cols = in.readInt();
        int size = rows * cols;
        this.size = size;
    }

    @Override
    public void deserialize(int start, int count, ExtendedDataInput in) throws IOException {}

    @Override
    public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {}

    public Entity get(int index){
        return new Void();
    }

    public Vector getSubVector(int[] indices) {
        int length = indices.length;
        return new BasicVoidVector(length);
    }

    public void set(int index, Entity value) throws Exception {}

    @Override
    public void set(int index, Object value) {}

    @Override
    public Vector combine(Vector vector) {
        return new BasicVoidVector(size + vector.rows());
    }

    @Override
    public boolean isNull(int index) {
        return true;
    }

    @Override
    public void setNull(int index) {}

    @Override
    public Entity.DATA_CATEGORY getDataCategory() {
        return DATA_CATEGORY.NOTHING;
    }

    @Override
    public Entity.DATA_TYPE getDataType() {
        return DATA_TYPE.DT_VOID;
    }

    @Override
    public Class<?> getElementClass() {
        return Void.class;
    }

    @Override
    public int rows() {
        return size;
    }

    protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {}

    @Override
    public int asof(Scalar value) {
        throw new RuntimeException("BasicVoidVector.asof not supported.");
    }

    @Override
    public int getUnitLength(){
        return 1;
    }

    @Override
    public void add(Object value) {
        throw new RuntimeException("BasicVoidVector not support add.");
    }

    @Override
    public void Append(Scalar value) throws Exception{
        size += 1;
    }

    @Override
    public void Append(Vector value) throws Exception{
        size += value.rows();
    }

    @Override
    public void checkCapacity(int requiredCapacity) {
        throw new RuntimeException("BasicVoidVector not support checkCapacity.");
    }

    @Override
    public ByteBuffer writeVectorToBuffer(ByteBuffer buffer) throws IOException {
        return buffer;
    }

    @Override
    public int serialize(int indexStart, int offect, int targetNumElement, AbstractVector.NumElementAndPartial numElementAndPartial, ByteBuffer out) throws IOException{
        numElementAndPartial.numElement = targetNumElement;
        numElementAndPartial.partial = 0;
        return targetNumElement;
    }

}
