package com.xxdb.route;

import com.xxdb.data.BasicInt;
import com.xxdb.data.Entity;
import com.xxdb.data.Utils;

public class DomainFactory {
    public static Domain createDomain(Entity.PARTITION_TYPE type, Entity.DATA_TYPE partitionColType, Entity values) throws Exception {
        if (type == Entity.PARTITION_TYPE.HASH) {
            Entity.DATA_TYPE dataType = partitionColType;
            Entity.DATA_CATEGORY dataCat = Utils.getCategory(dataType);
            int buckets = ((BasicInt)values).getInt();
            return new HashDomain(buckets, dataType, dataCat);
        } 
        throw new RuntimeException("Unsupported partition type " + type.toString());
    }
}
