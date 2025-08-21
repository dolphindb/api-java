package com.xxdb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

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
	
	protected BasicAnyVector(Entity[] array, boolean copy){
		super(DATA_FORM.DF_VECTOR);
		if(copy)
			values = array.clone();
		else
			values = array;
	}
	
	protected BasicAnyVector(ExtendedDataInput in) throws IOException{
		super(DATA_FORM.DF_VECTOR); 
		int rows = in.readInt();
		int cols = in.readInt();
		values = new Entity[rows];
		for (int i=0; i<rows; ++i) {
			short flag = in.readShort();
			int form = flag>>8;
			int type = flag & 0xff;
            boolean extended = type >= 128;
            if(type >= 128)
            	type -= 128;
			Entity obj = BasicEntityFactory.instance().createEntity(DATA_FORM.values()[form], DATA_TYPE.valueOf(type), in, extended);
			values[i] = obj;
		}
	}

	public Entity getEntity(int index){
		return values[index];
	}
	
	public Entity get(int index){
		return values[index];
	}
	
	public Vector getSubVector(int[] indices){
		int length = indices.length;
		Entity[] sub = new Entity[length];
		for(int i=0; i<length; ++i)
			sub[i] = values[indices[i]];
		return new BasicAnyVector(sub, false);
	}
	
	public void set(int index, Entity value) throws Exception {
		values[index] = value;
	}

	@Override
	public void set(int index, Object value) {
		throw new RuntimeException("BasicAnyVector.set(int index, Object value) not supported.");
	}

	public void setEntity(int index, Entity value){
		values[index] = value;
	}

	@Override
	public Vector combine(Vector vector) {
		throw new UnsupportedOperationException();
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

	@JsonIgnore
	@Override
	public int getUnitLength(){
		throw new RuntimeException("BasicAnyVector.getUnitLength not supported.");
	}

	public void addRange(Object[] valueList) {
		throw new RuntimeException("AnyVector not support addRange");
	}

	@Override
	public void Append(Scalar value) {
		throw new RuntimeException("AnyVector not support append");
	}

	@Override
	public void Append(Vector value) {
		throw new RuntimeException("AnyVector not support append");
	}

	@Override
	public void checkCapacity(int requiredCapacity) {
		throw new RuntimeException("BasicAnyVector not support checkCapacity.");
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
	public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {
		throw new RuntimeException("BasicAnyVector.serialize not supported.");
	}

	@Override
	public int serialize(int indexStart, int offect, int targetNumElement, AbstractVector.NumElementAndPartial numElementAndPartial, ByteBuffer out) throws IOException{
		throw new RuntimeException("BasicAnyVector.serialize not supported.");
	}

	public void add(Object value) {
		throw new RuntimeException("AnyVector not support add");
	}

	@Override
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {
		for(Entity value : values) {
			if (Objects.nonNull(value))
				value.write(out);
		}

	}
	
	@Override
	public int asof(Scalar value) {
		throw new RuntimeException("BasicAnyVector.asof not supported.");
	}
}
