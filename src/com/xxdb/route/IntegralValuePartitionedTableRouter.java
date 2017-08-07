package com.xxdb.route;

import com.xxdb.data.*;

import java.util.*;

class IntegralValuePartitionedTableRouter implements TableRouter {
    private Map<Long, List<String>> map;
    IntegralValuePartitionedTableRouter(AbstractVector values, BasicAnyVector locations) {
        if (values.getDataCategory() != Entity.DATA_CATEGORY.INTEGRAL) {
            throw new RuntimeException("expect a vector of integrals");
        }
        List<Long> longs = new ArrayList<>();
        for (int i = 0; i < values.rows(); ++i) {
            try {
                longs.add(Long.valueOf(values.get(i).getNumber().longValue()));
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        initialize(longs, locations);
    }

    private void initialize(List<Long> longs, BasicAnyVector locations) {
        if (locations.rows() <= 0) {
            throw new RuntimeException("requires at least one location");
        }
        if (locations.getEntity(0).getDataType() != Entity.DATA_TYPE.DT_STRING) {
            throw new RuntimeException("location must be a string");
        }
        if (longs.size() != locations.rows()) {
            throw new RuntimeException("expect # locations == # values");
        }
        map = new HashMap<>();
        boolean isScalar = locations.getEntity(0).getDataForm() == Entity.DATA_FORM.DF_SCALAR;
        if (isScalar) {
            for (int i = 0; i < locations.rows(); ++i) {
                BasicString location = (BasicString) locations.get(i);
                long val = longs.get(i);
                map.put(val, Arrays.asList(location.getString()));
            }
        } else {
            for (int i = 0; i < locations.rows(); ++i) {
                BasicStringVector locationVector = (BasicStringVector) locations.getEntity(i);
                long val = longs.get(i);
                map.put(val, new ArrayList<>());
                for (int j = 0; j < locationVector.rows(); ++j) {
                    map.get(val).add(locationVector.getString(j));
                }
            }
        }
    }

    @Override
    public String route(Scalar partitionColumn) {
        if (partitionColumn.getDataCategory() != Entity.DATA_CATEGORY.INTEGRAL)
            throw  new RuntimeException("invalid column category type" + partitionColumn.getDataCategory().name() + ", expect Integral category.");
        try {
            long longVal = partitionColumn.getNumber().longValue();
            List<String> locations = map.get(longVal);
            if (locations == null)
                throw new RuntimeException(partitionColumn.getNumber().longValue() + " does not match any partitioning values!");
            return locations.get(0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
