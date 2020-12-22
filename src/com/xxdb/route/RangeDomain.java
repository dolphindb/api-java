package com.xxdb.route;

import java.util.ArrayList;
import java.util.List;

import com.xxdb.data.Entity;
import com.xxdb.data.Vector;

public class RangeDomain implements Domain{
	private Entity.DATA_TYPE type;
	private Entity.DATA_CATEGORY cat;
	private Vector range;

	public RangeDomain(Vector range, Entity.DATA_TYPE type, Entity.DATA_CATEGORY cat){
		this.type = type;
		this.cat = cat;
		this.range = range;
	}
	
	@Override
	public List<Integer> getPartitionKeys(Vector partitionCol) {
		if(partitionCol.getDataCategory() != cat)
			throw new RuntimeException("Data category incompatible.");
		if(cat == Entity.DATA_CATEGORY.TEMPORAL && type != partitionCol.getDataType())
			throw new RuntimeException("Data type incompatible.");
		int partitions = range.rows() - 1;
 		int rows = partitionCol.rows();
		ArrayList<Integer> keys = new ArrayList<Integer>(rows);
		for(int i=0; i<rows; ++i){
			int index = range.asof(partitionCol.get(i));
			if(index >= partitions)
				keys.add(-1);
			else
				keys.add(index);
		}
		return keys;
	}
}
