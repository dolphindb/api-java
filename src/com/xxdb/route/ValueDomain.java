package com.xxdb.route;

import java.util.ArrayList;
import java.util.List;

import com.xxdb.data.Entity;
import com.xxdb.data.Scalar;
import com.xxdb.data.Utils;
import com.xxdb.data.Vector;
import com.xxdb.data.Entity.DATA_TYPE;

public class ValueDomain implements Domain{
	private Entity.DATA_TYPE type;
	private Entity.DATA_CATEGORY cat;

	public ValueDomain(Vector partitionScheme, Entity.DATA_TYPE type, Entity.DATA_CATEGORY cat){
		this.type = partitionScheme.getDataType();
		this.cat = partitionScheme.getDataCategory();
	}
	
	@Override
	public List<Integer> getPartitionKeys(Vector partitionCol) {
		if(partitionCol.getDataCategory() != cat)
			throw new RuntimeException("Data category incompatible.");
		if(cat == Entity.DATA_CATEGORY.TEMPORAL && type != partitionCol.getDataType()){
			DATA_TYPE old = partitionCol.getDataType();
			partitionCol = (Vector)Utils.castDateTime(partitionCol, type);
			if(partitionCol == null)
				throw new RuntimeException("Can't convert type from " + old.name() + " to " + type.name());
		}
		if(type == DATA_TYPE.DT_LONG)
			throw new RuntimeException("Long type value can't be used as a partition column.");
		
		int rows = partitionCol.rows();
		ArrayList<Integer> keys = new ArrayList<Integer>(rows);
		for(int i=0; i<rows; ++i)
			keys.add(partitionCol.hashBucket(i, 1048576));
		return keys;
	}

	@Override
	public int getPartitionKey(Scalar partitionCol) {
		if (partitionCol.getDataCategory() != cat)
			throw new RuntimeException("Data category incompatible.");
		if (cat == Entity.DATA_CATEGORY.TEMPORAL && type != partitionCol.getDataType())
		{
			DATA_TYPE old = partitionCol.getDataType();
			partitionCol = (Scalar)Utils.castDateTime(partitionCol, type);
			if (partitionCol == null)
				throw new RuntimeException("Can't convert type from " + old + " to " + type);
		}
		if (type == DATA_TYPE.DT_LONG)
			throw new RuntimeException("Long type value can't be used as a partition column.");
		int key = partitionCol.hashBucket(1048576);
		return key;
	}
}
