package com.xxdb.route;

import com.xxdb.data.BasicInt;
import com.xxdb.data.Entity;
import com.xxdb.data.Utils;
import com.xxdb.data.Vector;

public class DomainFactory {
    public static Domain createDomain(Entity.PARTITION_TYPE type, Entity.DATA_TYPE partitionColType, Entity values) throws Exception {
        if (type == Entity.PARTITION_TYPE.HASH) {
            Entity.DATA_TYPE dataType = partitionColType;
            Entity.DATA_CATEGORY dataCat = Utils.getCategory(dataType);
            int buckets = ((BasicInt)values).getInt();
            return new HashDomain(buckets, dataType, dataCat);
        } 
        else if (type == Entity.PARTITION_TYPE.VALUE) {
            Entity.DATA_TYPE dataType = partitionColType;
            Entity.DATA_CATEGORY dataCat = Utils.getCategory(dataType);
              return new ValueDomain(dataType, dataCat);
        } 
        else if (type == Entity.PARTITION_TYPE.RANGE) {
            Entity.DATA_TYPE dataType = partitionColType;
            Entity.DATA_CATEGORY dataCat = Utils.getCategory(dataType);
            return new RangeDomain((Vector)values, dataType, dataCat);
        } 
        else if (type == Entity.PARTITION_TYPE.LIST) {
            Entity.DATA_TYPE dataType = partitionColType;
            Entity.DATA_CATEGORY dataCat = Utils.getCategory(dataType);
            return new ListDomain((Vector)values, dataType, dataCat);
        } 
        throw new RuntimeException("Unsupported partition type " + type.toString());
    }
}
