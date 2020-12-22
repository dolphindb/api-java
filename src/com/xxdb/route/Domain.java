package com.xxdb.route;

import java.util.List;

import com.xxdb.data.Vector;

public interface Domain {
	/**
	 * Given the values of a partition column, get the keys of partitions those values belong to.
	 * @param partitionCol a list of partition column values
	 * @return a list of partition keys
	 */
	List<Integer> getPartitionKeys(Vector partitionCol);
	int getPartitionCount();
}
