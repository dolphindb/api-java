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
        BasicAnyVector anyVector = new BasicAnyVector(in);
        BasicIntVector intVector = (BasicIntVector) anyVector.get(0);
        int indexsLen = ((BasicInt) intVector.get(0)).getInt();
        int subVecNum = ((BasicInt) intVector.get(1)).getInt();
        int[] tmpIntArray = new int[indexsLen];
        System.arraycopy(intVector.getdataArray(), 2, tmpIntArray,0, indexsLen);
        indexs = new BasicIntVector(tmpIntArray);
        tmpIntArray = new int[indexsLen];
        System.arraycopy(intVector.getdataArray(), (indexsLen) + 2, tmpIntArray,0, indexsLen);
        indexsDataType = new BasicIntVector(tmpIntArray);

        subVector = new HashMap<>();
        for (int i = 1; i <= subVecNum; i++) {
            DATA_TYPE dataType = anyVector.get(i).getDataType();
            subVector.put(dataType.getValue(), anyVector.get(i));
        }
    }

    public Entity get(int index) {
        if (index >=rows())
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
        throw new RuntimeException("IotAnyVector.addRange not supported.");
    }

    @Override
    public void Append(Scalar value) {
        throw new RuntimeException("IotAnyVector.Append not supported.");
    }

    @Override
    public void Append(Vector value) {
        throw new RuntimeException("IotAnyVector.Append not supported.");
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
        int[] tmpIntArray = new int[rows() * 2 + 2];

        tmpIntArray[0] = rows();
        tmpIntArray[1] = subVector.size();

        System.arraycopy(indexs.getdataArray(), 0, tmpIntArray,2, rows());
        System.arraycopy(indexsDataType.getdataArray(), 0, tmpIntArray, rows() + 2, indexsDataType.size);
        BasicIntVector intVector = new BasicIntVector(tmpIntArray);

        Entity[] entities = new Entity[1 + subVector.size()];
        entities[0] = intVector;

        int index = 1;
        for (Entity value : subVector.values()) {
            entities[index] = value;
            index++;
        }

        BasicAnyVector anyVector = new BasicAnyVector(entities, false);
        anyVector.writeVectorToOutputStream(out);
    }

    @Override
    public int asof(Scalar value) {
        throw new RuntimeException("BasicAnyVector.asof not supported.");
    }
}
