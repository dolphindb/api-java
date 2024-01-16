package com.xxdb.data;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

/**
 * 
 * Corresponds to DolphinDB dictionary object
 *
 */

public class BasicDictionary extends AbstractEntity implements Dictionary{
	private Map<Entity, Entity> dict;
	private DATA_TYPE keyType;
	private DATA_TYPE valueType;
	
	public BasicDictionary(DATA_TYPE valueType, ExtendedDataInput in) throws IOException{
		this.valueType = valueType;
		
		//read key vector
		short flag = in.readShort();
		int form = flag>>8;
		int type = flag & 0xff;
        boolean extended = type >= 128;
        if(type >= 128)
        	type -= 128;
		if(form != DATA_FORM.DF_VECTOR.ordinal())
			throw new IOException("The form of dictionary keys must be vector");
		keyType = DATA_TYPE.valueOf(type);
		Vector keys = (Vector)BasicEntityFactory.instance().createEntity(DATA_FORM.DF_VECTOR, keyType, in, extended);
		
		//read value vector
		flag = in.readShort();
		form = flag>>8;
		type = flag & 0xff;
        extended = type >= 128;
        if(type >= 128)
        	type -= 128;
		if(form != DATA_FORM.DF_VECTOR.ordinal())
			throw new IOException("The form of dictionary values must be vector");
		valueType = DATA_TYPE.valueOf(type);
		Vector values = (Vector)BasicEntityFactory.instance().createEntity(DATA_FORM.DF_VECTOR, valueType, in, extended);
		
		if(keys.rows() != values.rows()){
			throw new IOException("The key size doesn't equate to value size.");
		}
		
		int size = keys.rows();
		int capacity = (int)(size/0.75);
		dict = new HashMap<Entity, Entity>(capacity);
		if(values.getDataType() == DATA_TYPE.DT_ANY){
			BasicAnyVector entityValues = (BasicAnyVector)values;
			for(int i=0; i<size; ++i)
				dict.put(keys.get(i), entityValues.getEntity(i));			
		}
		else{
			for(int i=0; i<size; ++i)
				dict.put(keys.get(i), values.get(i));
		}	
	}
	
	public BasicDictionary(DATA_TYPE keyType, DATA_TYPE valueType, int capacity){
		if(keyType == DATA_TYPE.DT_VOID || keyType == DATA_TYPE.DT_ANY || keyType == DATA_TYPE.DT_DICTIONARY)
			throw new IllegalArgumentException("Invalid keyType: " + keyType.name());
		this.keyType = keyType;
		this.valueType = valueType;
		dict = new HashMap<Entity, Entity>();
	}
	
	public BasicDictionary(DATA_TYPE keyType, DATA_TYPE valueType){
		this(keyType, valueType, 0);
	}
	
	@Override
	public DATA_FORM getDataForm() {
		return DATA_FORM.DF_DICTIONARY;
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return getDataCategory(valueType);
	}

	@Override
	public DATA_TYPE getDataType() {
		return valueType;
	}

	@Override
	public int rows() {
		return dict.size();
	}

	@Override
	public int columns() {
		return 1;
	}

	@Override
	public DATA_TYPE getKeyDataType() {
		return keyType;
	}

	@Override
	public Entity get(Scalar key) {
		return dict.get(key);
	}

	@Override
	public boolean put(Scalar key, Entity value) {
		if(key.getDataType() != getKeyDataType() || (value.getDataType() != getDataType()))
			return false;
		else{
			dict.put(key, value);
			return true;
		}
	}

	public Entity get(String key){
		return dict.get(new BasicString(key));
	}
	
	public Set<Entity> keys(){
		return dict.keySet();
	}
	
	public Collection<Entity> values(){
		return dict.values();
	}
	
	public Set<Map.Entry<Entity, Entity>> entrySet(){
		return dict.entrySet();
	}
	
	public String getString(){
		if(valueType == DATA_TYPE.DT_ANY){
			StringBuilder content = new StringBuilder();
			int count=0;
			Set<Map.Entry<Entity, Entity>> entries = dict.entrySet();
			Iterator<Map.Entry<Entity, Entity>> it = entries.iterator();
			while(it.hasNext() && count<20){
				Map.Entry<Entity, Entity> entry = it.next();
				content.append(entry.getKey().getString());
				content.append("->");
				DATA_FORM form = entry.getValue().getDataForm();
				if(form == DATA_FORM.DF_MATRIX || form == DATA_FORM.DF_TABLE)
					content.append("\n");
				else if(form == DATA_FORM.DF_DICTIONARY)
					content.append("{\n");
				content.append(entry.getValue().getString());
				if(form == DATA_FORM.DF_DICTIONARY)
					content.append("}");
				content.append("\n");
				++count;
			}
			if(it.hasNext())
				content.append("...\n");
			return content.toString();
		}
		else{
			StringBuilder sbKeys = new StringBuilder("{");
			StringBuilder sbValues = new StringBuilder("{");
			Set<Map.Entry<Entity, Entity>> entries = dict.entrySet();
			Iterator<Map.Entry<Entity, Entity>> it = entries.iterator();
			if(it.hasNext()){
				Map.Entry<Entity, Entity> entry = it.next();
				sbKeys.append(entry.getKey().getString());
				sbValues.append(entry.getValue().getString());
			}
			int count=1;
			while(it.hasNext() && count<20){
				Map.Entry<Entity, Entity> entry = it.next();
				sbKeys.append(',');
				sbKeys.append(entry.getKey().getString());
				sbValues.append(',');
				sbValues.append(entry.getValue().getString());
				++count;
			}
			if(it.hasNext()){
				sbKeys.append("...");
				sbValues.append("...");
			}
			sbKeys.append("}");
			sbValues.append("}");
			return sbKeys.toString() + "->" + sbValues.toString();
		}
	}
	
	public void write(ExtendedDataOutput out) throws IOException{
		if(valueType==DATA_TYPE.DT_DICTIONARY)
			throw new IOException("Can't streamlize the dictionary with value type " + valueType.name());
		
		BasicEntityFactory factory = new BasicEntityFactory();
		Vector keys = (Vector)factory.createVectorWithDefaultValue(keyType, dict.size(), -1);
		Vector values = (Vector)factory.createVectorWithDefaultValue(valueType, dict.size(), -1);
		int index = 0;
		try{
			for(Map.Entry<Entity, Entity> entry : dict.entrySet()){
				keys.set(index, entry.getKey());
				values.set(index, (Scalar)entry.getValue());
				++index;
			}
		}
		catch(Exception ex){
			throw new IOException(ex.getMessage());
		}
		
		int flag = (DATA_FORM.DF_DICTIONARY.ordinal() << 8) + getDataType().getValue();
		out.writeShort(flag);
		
		keys.write(out);
		values.write(out);
	}
}
