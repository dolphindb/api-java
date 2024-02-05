package com.xxdb.route;

import java.util.ArrayList;
import java.util.List;

import com.xxdb.data.Entity;
import com.xxdb.data.Scalar;
import com.xxdb.data.Utils;
import com.xxdb.data.Vector;
import com.xxdb.data.Entity.DATA_TYPE;

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
		if (partitionCol.getDataCategory() != this.cat)
			throw new RuntimeException("Data category incompatible.");
		if (cat == Entity.DATA_CATEGORY.TEMPORAL && this.type != partitionCol.getDataType())
			partitionCol = (Vector)Utils.castDateTime(partitionCol, this.type);

		int partitions = this.range.rows() - 1;
 		int rows = partitionCol.rows();
		ArrayList<Integer> keys = new ArrayList<Integer>(rows);
		for(int i=0; i<rows; ++i){
			int index = this.range.asof(((Scalar) partitionCol.get(i)));
			if(index >= partitions)
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

		int partitions = this.range.rows() - 1;
		int key = this.range.asof(partitionCol);
		if (key >= partitions)
			key = -1;
		return key;
	}
}
