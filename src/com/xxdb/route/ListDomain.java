package com.xxdb.route;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.xxdb.data.BasicAnyVector;
import com.xxdb.data.Entity;
import com.xxdb.data.Scalar;
import com.xxdb.data.Utils;
import com.xxdb.data.Vector;
import com.xxdb.data.Entity.DATA_TYPE;

public class ListDomain implements Domain {
	private Entity.DATA_TYPE type;
	private Entity.DATA_CATEGORY cat;
	private Map<Entity, Integer> dict;

	public ListDomain(Vector list, Entity.DATA_TYPE type, Entity.DATA_CATEGORY cat){
		this.type = type;
		this.cat = cat;
		if(list.getDataType() != Entity.DATA_TYPE.DT_ANY)
			throw new RuntimeException("The input list must be a tuple.");
		this.dict = new HashMap<Entity, Integer>();
		BasicAnyVector values = (BasicAnyVector)list;
		int partitions = values.rows();
		for(int i=0; i<partitions; ++i){
			Entity cur = values.getEntity(i);
			if(cur.isScalar())
				this.dict.put((Scalar)cur, i);
			else{
				Vector vec = (Vector)cur;
				for(int j=0; j<vec.rows(); ++j)
					this.dict.put(vec.get(j), i);
			}
		}
	}
	
	@Override
	public List<Integer> getPartitionKeys(Vector partitionCol) {
		if(partitionCol.getDataCategory() != this.cat)
			throw new RuntimeException("Data category incompatible.");
		if (this.cat == Entity.DATA_CATEGORY.TEMPORAL && type != partitionCol.getDataType())
			partitionCol = (Vector)Utils.castDateTime(partitionCol, this.type);
		
		int rows = partitionCol.rows();
		ArrayList<Integer> keys = new ArrayList<Integer>(rows);
		for(int i=0; i<rows; ++i){
			Integer index = this.dict.get(partitionCol.get(i));
			if(index == null)
				keys.add(-1);
			else
				keys.add(index);
		}
		return keys;
	}

	@Override
	public int getPartitionKey(Scalar partitionCol) {
		if (partitionCol.getDataCategory() != this.cat)
			throw new RuntimeException("Data category incompatible.");
		if (this.cat == Entity.DATA_CATEGORY.TEMPORAL && this.type != partitionCol.getDataType())
			partitionCol = (Scalar)Utils.castDateTime(partitionCol, this.type);

		int index = 0;
		index = this.dict.getOrDefault(partitionCol, -1);
		return index;
	}
}
