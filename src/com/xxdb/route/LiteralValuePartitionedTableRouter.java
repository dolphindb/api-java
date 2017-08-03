package com.xxdb.route;

import com.xxdb.data.*;

import java.util.*;

class LiteralValuePartitionedTableRouter implements TableRouter {
    private Map<String, List<String>> map;
    LiteralValuePartitionedTableRouter(AbstractVector values, BasicAnyVector locations) {
        if (values.getDataCategory() != Entity.DATA_CATEGORY.LITERAL) {
            throw new RuntimeException("expect a vector of literals");
        }
        List<String> strings = new ArrayList<>();
        for (int i = 0; i < values.rows(); ++i) {
            try {
                strings.add(values.get(i).getString());
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        initialize(strings, locations);
    }

    private void initialize(List<String> strings, BasicAnyVector locations) {
        if (locations.rows() <= 0) {
            throw new RuntimeException("requires at least one location");
        }
        if (locations.getEntity(0).getDataType() != Entity.DATA_TYPE.DT_STRING) {
            throw new RuntimeException("location must be a string");
        }
        if (strings.size() != locations.rows()) {
            throw new RuntimeException("expect # locations == # values");
        }
        map = new HashMap<>();
        boolean isScalar = locations.getEntity(0).getDataForm() == Entity.DATA_FORM.DF_SCALAR;
        if (isScalar) {
            for (int i = 0; i < locations.rows(); ++i) {
                BasicString location = (BasicString) locations.get(i);
                String val = strings.get(i);
                map.put(val, Arrays.asList(location.getString()));
            }
        } else {
            for (int i = 0; i < locations.rows(); ++i) {
                BasicStringVector locationVector = (BasicStringVector) locations.getEntity(i);
                String val = strings.get(i);
                map.put(val, new ArrayList<>());
                for (int j = 0; j < locationVector.rows(); ++j) {
                    map.get(val).add(locationVector.getString(j));
                }
            }
        }
    }

    @Override
    public String route(Scalar partitionColumn) {
        if (partitionColumn.getDataCategory() != Entity.DATA_CATEGORY.LITERAL)
            throw  new RuntimeException("invalid column category type" + partitionColumn.getDataCategory().name() + ", expect Literal category.");
        try {
            String stringVal = partitionColumn.getString();
            List<String> locations = map.get(stringVal);
            if (locations == null)
                throw new RuntimeException(partitionColumn.getString() + " does not match any partitioning values!");
            return locations.get(0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
