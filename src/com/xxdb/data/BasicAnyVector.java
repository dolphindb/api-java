package com.xxdb.data;

import java.io.IOException;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * 
 * Corresponds to DolphinDB tuple(any vector)
 *
 */

public class BasicAnyVector extends AbstractVector{
	private Entity[] values;
	

	public BasicAnyVector(int size){
		super(DATA_FORM.DF_VECTOR);
		values = new Entity[size];
	}
	
	protected BasicAnyVector(ExtendedDataInput in) throws IOException{
		super(DATA_FORM.DF_VECTOR); 
		int rows = in.readInt();
		int cols = in.readInt();
		int size = rows * cols;
		values = new Entity[size];
		assert(rows <= 1024);
		BasicEntityFactory factory = new BasicEntityFactory();
		for(int i=0; i<size; ++i){
			short flag = in.readShort();
			int form = flag>>8;
			int type = flag & 0xff;
			//if (form != 1)
				//assert (form == 1);
			//if (type != 4)
				//assert(type == 4);
			Entity obj = factory.createEntity(DATA_FORM.values()[form], DATA_TYPE.values()[type], in);
			values[i] = obj;
		}

	}

	public Entity getEntity(int index){
		return values[index];
	}
	
	public Scalar get(int index){
		if(values[index].isScalar())
			return (Scalar)values[index];
		else
			throw new RuntimeException("The element of the vector is not a scalar object.");
	}
	
	public void set(int index, Scalar value) throws Exception {
		values[index] = value;
	}
	
	public void setEntity(int index, Entity value){
		values[index] = value;
	}

	@Override
	public Vector combine(Vector vector) {
		throw new NotImplementedException();
	}

	@Override
	public boolean isNull(int index) {
		return values[index] == null || (values[index].isScalar() && ((Scalar)values[index]).isNull());
	}

	@Override
	public void setNull(int index) {
		values[index] = new Void();
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
		return values.length;
	}
	
	public String getString(){
		StringBuilder sb = new StringBuilder("(");
		int size = Math.min(10, rows());
		if(size > 0)
			sb.append(getEntity(0).getString());
		for(int i=1; i<size; ++i){
			sb.append(',');
			sb.append(getEntity(i).getString());
		}
		if(size < rows())
			sb.append(",...");
		sb.append(")");
		return sb.toString();
	}
	
	public Class<?> getElementClass(){
		return Entity.class;
	}

	@Override
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {
		for(Entity value : values)
			value.write(out);
	}
}
