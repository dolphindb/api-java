package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class BasicIotAnyVector extends AbstractVector {

    private Map<Integer, Entity> subVector;
    private BasicIntVector indexsDataType;
    private BasicIntVector indexs;

    private static final Logger log = LoggerFactory.getLogger(BasicIotAnyVector.class);


    public BasicIotAnyVector(Scalar[] scalars) {
        super(DATA_FORM.DF_VECTOR);

        if (Objects.isNull(scalars) || scalars.length == 0)
            throw new RuntimeException("The param 'scalars' cannot be null or empty.");

        subVector = new HashMap<>();
        indexsDataType = new BasicIntVector(0);
        indexs = new BasicIntVector(0);

        try {
            for (Scalar scalar : scalars) {
                int curDataTypeValue = scalar.getDataType().getValue();

                if (Objects.isNull(subVector.get(curDataTypeValue))) {
                    Vector curVector = BasicEntityFactory.instance().createVectorWithDefaultValue(scalar.getDataType(), 0, -1);
                    curVector.Append(scalar);
                    subVector.put(curDataTypeValue, curVector);
                } else {
                    ((Vector) subVector.get(curDataTypeValue)).Append(scalar);
                }

                indexsDataType.add(curDataTypeValue);
                indexs.add(subVector.get(curDataTypeValue).rows() - 1);
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    protected BasicIotAnyVector(ExtendedDataInput in) throws IOException {
        super(DATA_FORM.DF_VECTOR);
        int rows = in.readInt();
        int cols = in.readInt(); // 1
        long size = in.readInt();

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
        if (index >= rows())
            throw new RuntimeException(String.format("index %s out of rows %s.", index, rows()));

        BasicInt curDataType = (BasicInt) indexsDataType.get(index);
        BasicInt curIndex = (BasicInt) indexs.get(index);

        return ((Vector) subVector.get(curDataType.getInt())).get(curIndex.getInt());
    }

    public String getString(int index) {
        return get(index).getString();
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
        throw new RuntimeException("IotAnyVector not support addRange.");
    }

    @Override
    public void Append(Scalar value) {
        throw new RuntimeException("IotAnyVector not support append");
    }

    @Override
    public void Append(Vector value) {
        throw new RuntimeException("IotAnyVector not support append.");
    }

    public String getString(){
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < rows(); i++)
            sb.append(getString(i)).append(",");

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
        int size = indexs.rows();
        out.writeInt(size);

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
