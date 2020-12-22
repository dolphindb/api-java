package com.xxdb.route;

import java.util.ArrayList;
import java.util.List;

import com.xxdb.data.Entity;
import com.xxdb.data.Vector;

public class HashDomain implements Domain{
	private int buckets;
	private Entity.DATA_TYPE type;
	private Entity.DATA_CATEGORY cat;

	public HashDomain(int buckets, Entity.DATA_TYPE type, Entity.DATA_CATEGORY cat){
		this.buckets = buckets;
		this.type = type;
		this.cat = cat;
	}
	
	@Override
	public List<Integer> getPartitionKeys(Vector partitionCol) {
		if(partitionCol.getDataCategory() != cat)
			throw new RuntimeException("Data category incompatible.");
		if(cat == Entity.DATA_CATEGORY.TEMPORAL && type != partitionCol.getDataType())
			throw new RuntimeException("Data type incompatible.");
		
		int rows = partitionCol.rows();
		ArrayList<Integer> keys = new ArrayList<Integer>(rows);
		for(int i=0; i<rows; ++i)
			keys.add(partitionCol.hashBucket(i, buckets));
		return keys;
	}
	
	@Override
	public int getPartitionCount(){
		return buckets;
	}
}
