package com.xxdb.route;

import com.xxdb.data.*;

import java.util.*;

class IntegralListPartitionedTableRouter implements TableRouter {
    private Map<Long, List<String>> locationMap;
    IntegralListPartitionedTableRouter(AbstractVector values, BasicAnyVector locations) {
        initialize(values, locations);
    }

    private void initialize(AbstractVector values, BasicAnyVector locations) {
        if (values.getDataType() != Entity.DATA_TYPE.DT_ANY || values.getDataForm() != Entity.DATA_FORM.DF_VECTOR) {
            throw new RuntimeException("expect a vector of partitioning lists");
        }
        if (values.rows() <= 0) {
            throw new RuntimeException("requires at least one partitioning list");
        }
        if (locations.rows() <= 0) {
            throw new RuntimeException("requires at least one location");
        }
        if (locations.getEntity(0).getDataType() != Entity.DATA_TYPE.DT_STRING) {
            throw new RuntimeException("location must be a string");
        }
        if (values.rows() != locations.rows()) {
            throw new RuntimeException("expect # locations == # partitioning lists");
        }
        this.locationMap = new HashMap<>();
        List<String>[] locationListArray = new List[locations.rows()];
        boolean isScalar = locations.getEntity(0).getDataForm() == Entity.DATA_FORM.DF_SCALAR;
        if (isScalar) {
            for (int i = 0; i < locations.rows(); ++i) {
                BasicString location = (BasicString)locations.get(i);
                locationListArray[i] = Arrays.asList(location.getString());
            }
        } else {
            for (int i = 0; i < locations.rows(); ++i) {
                BasicStringVector locationVector = (BasicStringVector) locations.getEntity(i);
                locationListArray[i] = new ArrayList<>();
                for (int j = 0; j < locationVector.rows(); ++j) {
                    BasicString location = (BasicString)locationVector.get(j);
                    locationListArray[i].add(location.getString());
                }
            }
        }

        for (int i = 0; i < values.rows(); ++i) {
            AbstractVector partitioningList = (AbstractVector) ((BasicAnyVector) values).getEntity(i);
            if (partitioningList.rows() <= 0) {
                throw new RuntimeException("expect partitioning list to be nonempty");
            }
            if (partitioningList.getDataCategory() != Entity.DATA_CATEGORY.INTEGRAL) {
                throw new RuntimeException("expect partitioning column values to be integral-typed.");
            }
            for (int j = 0; j < partitioningList.rows(); ++j) {
                try {
                    long val = partitioningList.get(j).getNumber().longValue();
                    locationMap.put(val, locationListArray[i]);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public String route(Scalar partitionColumn) {
        if (partitionColumn.getDataCategory() != Entity.DATA_CATEGORY.INTEGRAL)
            throw  new RuntimeException("invalid column category type" + partitionColumn.getDataCategory().name() + ", expect integral category.");
        try {
            long longVal = partitionColumn.getNumber().longValue();
            List<String> locations = locationMap.get(longVal);
            if (locations == null)
                throw new RuntimeException(partitionColumn.getNumber().longValue() + " not in any partitioning list!");
            return locations.get(0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
