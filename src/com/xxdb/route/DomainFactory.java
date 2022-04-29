package com.xxdb.route;

import com.xxdb.data.*;

public class DomainFactory {
    public static Domain createDomain(Entity.PARTITION_TYPE type, Entity.DATA_TYPE partitionColType, Entity partitionSchema) throws Exception {
        if (type == Entity.PARTITION_TYPE.HASH) {
            Entity.DATA_TYPE dataType = partitionColType;
            Entity.DATA_CATEGORY dataCat = Utils.getCategory(dataType);
            int buckets = ((BasicInt)partitionSchema).getInt();
            return new HashDomain(buckets, dataType, dataCat);
        } 
        else if (type == Entity.PARTITION_TYPE.VALUE) {
            Entity.DATA_TYPE dataType = partitionSchema.getDataType();
            Entity.DATA_CATEGORY dataCat = Utils.getCategory(dataType);
              return new ValueDomain((Vector)partitionSchema, dataType, dataCat);
        } 
        else if (type == Entity.PARTITION_TYPE.RANGE) {
        	Entity.DATA_TYPE dataType = partitionSchema.getDataType();
            Entity.DATA_CATEGORY dataCat = Utils.getCategory(dataType);
            return new RangeDomain((Vector)partitionSchema, dataType, dataCat);
        } 
        else if (type == Entity.PARTITION_TYPE.LIST) {
            Entity.DATA_TYPE dataType;
            if (partitionSchema.getDataType() == Entity.DATA_TYPE.DT_ANY){
                dataType = ((BasicAnyVector)partitionSchema).getEntity(0).getDataType();
            }else {
                dataType = partitionSchema.getDataType();
            }
            Entity.DATA_CATEGORY dataCat = Utils.getCategory(dataType);
            return new ListDomain((Vector)partitionSchema, dataType, dataCat);
        } 
        throw new RuntimeException("Unsupported partition type " + type.toString());
    }
}
