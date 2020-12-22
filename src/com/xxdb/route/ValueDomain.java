package com.xxdb.route;

import java.util.ArrayList;
import java.util.List;

import com.xxdb.data.Entity;
import com.xxdb.data.Vector;
import com.xxdb.data.Entity.DATA_TYPE;

public class ValueDomain implements Domain{
	private Entity.DATA_TYPE type;
	private Entity.DATA_CATEGORY cat;

	public ValueDomain(Entity.DATA_TYPE type, Entity.DATA_CATEGORY cat){
		this.type = type;
		this.cat = cat;
	}
	
	@Override
	public List<Integer> getPartitionKeys(Vector partitionCol) {
		if(partitionCol.getDataCategory() != cat)
			throw new RuntimeException("Data category incompatible.");
		if(cat == Entity.DATA_CATEGORY.TEMPORAL && type != partitionCol.getDataType())
			throw new RuntimeException("Data type incompatible.");
		if(type == DATA_TYPE.DT_LONG)
			throw new RuntimeException("Long type value can't be used as a partition column.");
		
		int rows = partitionCol.rows();
		ArrayList<Integer> keys = new ArrayList<Integer>(rows);
		for(int i=0; i<rows; ++i)
			keys.add(partitionCol.hashBucket(i, 1048576));
		return keys;
	}
}
