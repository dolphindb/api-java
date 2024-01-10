package com.xxdb.data;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

/**
 * 
 * Corresponds to DolphinDB set object
 *
 */

public class BasicSet extends AbstractEntity implements Set {
	private java.util.Set<Entity> set;
	private DATA_TYPE keyType;
	
	public BasicSet(DATA_TYPE keyType, ExtendedDataInput in) throws IOException{
		this.keyType = keyType;
		
		//read key vector
		short flag = in.readShort();
		int form = flag>>8;
		int type = flag & 0xff;
        boolean extended = type >= 128;
        if(type >= 128)
        	type -= 128;
		if(form != DATA_FORM.DF_VECTOR.ordinal())
			throw new IOException("The form of set keys must be vector");
		if(type <0 || type >= DATA_TYPE.DT_OBJECT.getValue())
			throw new IOException("Invalid key type: " + type);
		
		Vector keys = (Vector)BasicEntityFactory.instance().createEntity(DATA_FORM.DF_VECTOR, DATA_TYPE.valueOf(type), in, extended, -1);
			
		int size = keys.rows();
		int capacity = (int)(size/0.75);
		set = new HashSet<Entity>(capacity);
		for(int i=0; i<size; ++i)
			set.add(keys.get(i));
	}
	
	public BasicSet(DATA_TYPE keyType, int capacity){
		if(keyType == DATA_TYPE.DT_VOID || keyType == DATA_TYPE.DT_SYMBOL || keyType.getValue() >= DATA_TYPE.DT_FUNCTIONDEF.getValue())
			throw new IllegalArgumentException("Invalid keyType: " + keyType.name());
		this.keyType = keyType;
		set = new HashSet<Entity>(capacity);
	}
	
	public BasicSet(DATA_TYPE keyType){
		this(keyType, 0);
	}
	
	@Override
	public DATA_FORM getDataForm() {
		return DATA_FORM.DF_SET;
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return getDataCategory(keyType);
	}

	@Override
	public DATA_TYPE getDataType() {
		return keyType;
	}

	@Override
	public int rows() {
		return set.size();
	}

	@Override
	public int columns() {
		return 1;
	}
	
	public Vector keys(){
		return keys(set.size());
	}
	
	private Vector keys(int top){
		int size = Math.min(top, set.size());
		Vector keys = (Vector)BasicEntityFactory.instance().createVectorWithDefaultValue(keyType, size, -1);
		Iterator<Entity> it = set.iterator();
		int count = 0;
		try{
			while(count<size)
				keys.set(count++, it.next());
		}
		catch(Exception ex){
			return null;
		}
		return keys;
	}
	
	@Override
	public boolean contains(Scalar key) {
		return set.contains(key);
	}

	@Override
	public boolean add(Scalar key) {
		if(key.getDataType() != keyType)
			return false;
		return set.add(key);
	}
	
	public String getString(){
		return keys(Vector.DISPLAY_ROWS).getString();
	}
	
	public void write(ExtendedDataOutput out) throws IOException{
		Vector keys = keys();	
		int flag = (DATA_FORM.DF_SET.ordinal() << 8) + getDataType().getValue();
		out.writeShort(flag);
		keys.write(out);
	}
}
