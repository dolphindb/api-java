package com.xxdb.data;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;

public class FetchAnyVector extends AbstractVector{
    private Entity currentValue ;
    private int size ;
    private int currentIndex = 0;
    private ExtendedDataInput instream;

    public FetchAnyVector(ExtendedDataInput in) throws IOException{
        super(DATA_FORM.DF_VECTOR);
        int rows = in.readInt();
        int cols = in.readInt();
        size = rows * cols;
        currentIndex = 0;
        instream = in;
    }

    public Entity read() throws IOException{
        BasicEntityFactory factory = new BasicEntityFactory();
        if(currentIndex>=size) return null;

        short flag = instream.readShort();
        int form = flag >> 8;
        int type = flag & 0xff;
        currentValue = factory.createEntity(DATA_FORM.values()[form], DATA_TYPE.values()[type], instream);
        currentIndex++;
        return currentValue;
    }

    public void skip() throws IOException{
        BasicEntityFactory factory = new BasicEntityFactory();
        for(int i=currentIndex;i<size;i++){
            short flag = instream.readShort();
            int form = flag >> 8;
            int type = flag & 0xff;
            currentValue = factory.createEntity(DATA_FORM.values()[form], DATA_TYPE.values()[type], instream);
            currentIndex++;
        }
    }
    public Scalar get(int index){
        throw new RuntimeException("The element of the vector is not a scalar object.");
    }

    public void set(int index, Scalar value) throws Exception {
        throw new RuntimeException("The element of the vector is not a scalar object.");
    }
    @Override
    public Vector combine(Vector vector) {
        throw new NotImplementedException();
    }

    @Override
    public boolean isNull(int index) {
        return currentValue == null || (currentValue.isScalar() && ((Scalar)currentValue).isNull());
    }

    @Override
    public void setNull(int index) {
        throw new RuntimeException("The element of the fetch vector is not allowed to update");
    }

    @Override
    public DATA_CATEGORY getDataCategory() {
        return Entity.DATA_CATEGORY.MIXED;
    }

    @Override
    public DATA_TYPE getDataType() {
        return Entity.DATA_TYPE.DT_ANY;
    }

    @Override
    public int rows() {
        return currentValue.rows();
    }

    public Class<?> getElementClass(){
        return Entity.class;
    }

    @Override
    protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {
        currentValue.write(out);
    }
}
