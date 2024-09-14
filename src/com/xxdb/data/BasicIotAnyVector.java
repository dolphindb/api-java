package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class BasicIotAnyVector extends AbstractVector {

    private Map<Integer, Entity> subVector;
    private BasicIntVector indexsDataType;
    private BasicIntVector indexs;

    protected BasicIotAnyVector(Entity[] array) {
        super(DATA_FORM.DF_VECTOR);
        subVector = new HashMap<>();
        indexsDataType = new BasicIntVector(0);
        indexs = new BasicIntVector(0);
        for (int i = 0; i < array.length; i++) {
            subVector.put(array[i].getDataType().getValue(), array[i]);
            for (int j = 0; j < array[i].rows(); j++ ) {
                indexsDataType.add(array[i].getDataType().getValue());
                indexs.add(j);
            }
        }
    }

    protected BasicIotAnyVector(ExtendedDataInput in) throws IOException {
        super(DATA_FORM.DF_VECTOR);
        int rows = in.readInt();
        int cols = in.readInt(); // 1
        long size = in.readLong();

        indexsDataType = new BasicIntVector(0);
        indexs = new BasicIntVector(0);
        for(long i = 0; i < size; i++){
            int type = in.readInt();
            int idx = in.readInt();
            indexsDataType.add(type);
            indexs.add(idx);
        }

        int typeSize = in.readInt();
        subVector = new HashMap<>();
        for (int i = 0; i < typeSize; i++) {
            short flag = in.readShort();
            int form = flag>>8;
            int type = flag & 0xff;
            boolean extended = type >= 128;
            if(type >= 128)
                type -= 128;
            Entity obj = BasicEntityFactory.instance().createEntity(DATA_FORM.values()[form], DATA_TYPE.valueOf(type), in, extended);
            subVector.put(type, obj);
        }
    }

    public Entity get(int index) {
        BasicInt dataType = (BasicInt) indexsDataType.get(index);
        return subVector.get(dataType.getInt());
    }

    public String getString(int index) {
        BasicInt curDataType = (BasicInt) indexsDataType.get(index);
        BasicInt curIndex = (BasicInt) indexs.get(index);

        return ((Vector) subVector.get(curDataType.getInt())).get(curIndex.getInt()).getString();
    }

    public Vector getSubVector(int[] indices){
        throw new RuntimeException("BasicIotAnyVector.getSubVector not supported.");
    }

    public void set(int index, Entity value) throws Exception {
        throw new RuntimeException("BasicIotAnyVector.set not supported.");
    }

    @Override
    public Vector combine(Vector vector) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int index) {
        throw new RuntimeException("BasicIotAnyVector.isNull not supported.");
    }

    @Override
    public void setNull(int index) {
        throw new RuntimeException("BasicIotAnyVector.setNull not supported.");
    }

    @Override
    public DATA_CATEGORY getDataCategory() {
        return Entity.DATA_CATEGORY.MIXED;
    }

    @Override
    public DATA_TYPE getDataType() {
        return DATA_TYPE.DT_IOTANY;
    }

    @Override
    public int rows() {
        return indexs.rows();
    }

    @JsonIgnore
    @Override
    public int getUnitLength(){
        throw new RuntimeException("IotAnyVector.getUnitLength not supported.");
    }

    public void addRange(Object[] valueList) {
        throw new RuntimeException("IotAnyVector not support addRange");
    }

    @Override
    public void Append(Scalar value) {
        throw new RuntimeException("IotAnyVector not support append");
    }

    @Override
    public void Append(Vector value) {
        throw new RuntimeException("IotAnyVector not support append");
    }

    public String getString(){
        StringBuilder sb = new StringBuilder("[");
        for (Entity value : subVector.values())
            sb.append(value.getString()).append(",");

        sb.setLength(sb.length() - 1);
        sb.append("]");

        return sb.toString();
    }

    public Class<?> getElementClass(){
        return Entity.class;
    }

    @Override
    public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {
        throw new RuntimeException("BasicIotAnyVector.serialize not supported.");
    }

    @Override
    public int serialize(int indexStart, int offect, int targetNumElement, AbstractVector.NumElementAndPartial numElementAndPartial, ByteBuffer out) throws IOException{
        throw new RuntimeException("BasicIotAnyVector.serialize not supported.");
    }

    @Override
    protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {
        long size = indexs.rows();
        out.writeLong(size);

        for (int i = 0; i < size; i++) {
            out.writeInt(indexsDataType.getInt(i));
            out.writeInt(indexs.getInt(i));
        }

        out.writeInt(subVector.size());

        for (Entity value : subVector.values()) {
            if (Objects.nonNull(value))
                value.write(out);
        }
    }

    @Override
    public int asof(Scalar value) {
        throw new RuntimeException("BasicAnyVector.asof not supported.");
    }
}
