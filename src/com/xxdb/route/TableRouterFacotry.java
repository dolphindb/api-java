package com.xxdb.route;

import com.xxdb.data.AbstractVector;
import com.xxdb.data.BasicAnyVector;
import com.xxdb.data.Entity;

public class TableRouterFacotry {
    public static TableRouter createRouter(Entity.PARTITION_TYPE type, AbstractVector values, BasicAnyVector locations) {
        if (type == Entity.PARTITION_TYPE.RANGE) {
            if (values.getDataCategory() == Entity.DATA_CATEGORY.INTEGRAL) {
                return new IntegralRangePartitionedTableRouter(values, locations);
            } else if (values.getDataCategory() == Entity.DATA_CATEGORY.LITERAL) {
                return new LiteralRangePartitionedTableRouter(values, locations);
            }
        } else if (type == Entity.PARTITION_TYPE.VALUE) {
            if (values.getDataCategory() == Entity.DATA_CATEGORY.INTEGRAL) {
                return new IntegralValuePartitionedTableRouter(values, locations);
            } else if (values.getDataCategory() == Entity.DATA_CATEGORY.LITERAL) {
                return new LiteralValuePartitionedTableRouter(values, locations);
            }
        } else if (type == Entity.PARTITION_TYPE.LIST){
            BasicAnyVector schema = (BasicAnyVector)values;
            if (schema.getEntity(0).getDataCategory() == Entity.DATA_CATEGORY.INTEGRAL) {
                return new IntegralListPartitionedTableRouter(values, locations);
            } else if (schema.getEntity(0).getDataCategory() == Entity.DATA_CATEGORY.LITERAL) {
                return new LiteralListPartitionedTableRouter(values, locations);
            }
        }
        throw new RuntimeException("Unsupported partition type " + type.toString());
    }
}
