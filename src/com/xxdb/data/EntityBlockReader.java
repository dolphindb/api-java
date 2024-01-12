package com.xxdb.data;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

import java.io.IOException;

public class EntityBlockReader implements Entity {
    private Entity currentValue ;
    private int size ;
    private int currentIndex = 0;
    private ExtendedDataInput instream;

    public EntityBlockReader(ExtendedDataInput in) throws IOException{
        int rows = in.readInt();
        int cols = in.readInt();
        size = rows * cols;
        currentIndex = 0;
        instream = in;
    }

    public Entity read() throws IOException{
        if(currentIndex>=size) return null;

        short flag = instream.readShort();
        int form = flag >> 8;
        int type = flag & 0xff;
        boolean extended = type >= 128;
        if(type >= 128)
        	type -= 128;
        currentValue = BasicEntityFactory.instance().createEntity(Entity.DATA_FORM.values()[form], Entity.DATA_TYPE.valueOf(type), instream, extended, -1);
        currentIndex++;
        return currentValue;
    }

    public void skipAll() throws IOException{
        for(int i=currentIndex;i<size;i++){
            short flag = instream.readShort();
            int form = flag >> 8;
            int type = flag & 0xff;
            boolean extended = type >= 128;
            if(type >= 128)
            	type -= 128;
            currentValue = BasicEntityFactory.instance().createEntity(Entity.DATA_FORM.values()[form], Entity.DATA_TYPE.valueOf(type), instream, extended, -1);
            currentIndex++;
        }
    }
    public Boolean hasNext(){
        return currentIndex < size;
    }

    @Override
    public DATA_FORM getDataForm() {
        return DATA_FORM.DF_TABLE;
    }

    @Override
    public DATA_CATEGORY getDataCategory() {
        return DATA_CATEGORY.MIXED;
    }

    @Override
    public DATA_TYPE getDataType() {
        return DATA_TYPE.DT_ANY;
    }

    @Override
    public int rows() {
        return 0;
    }

    @Override
    public int columns() {
        return 0;
    }

    @Override
    public String getString() {
        return null;
    }

    @Override
    public void write(ExtendedDataOutput output) throws IOException {

    }

    @Override
    public boolean isScalar() {
        return false;
    }

    @Override
    public boolean isVector() {
        return false;
    }

    @Override
    public boolean isPair() {
        return false;
    }

    @Override
    public boolean isTable() {
        return true;
    }

    @Override
    public boolean isMatrix() {
        return false;
    }

    @Override
    public boolean isDictionary() {
        return false;
    }

    @Override
    public boolean isChart() {
        return false;
    }

    @Override
    public boolean isChunk() {
        return false;
    }
}
