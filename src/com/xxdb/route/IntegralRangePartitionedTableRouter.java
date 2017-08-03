package com.xxdb.route;

import com.xxdb.data.*;

import java.util.*;

class IntegralRangePartitionedTableRouter extends AbstractRangePartitionedTableRouter {
    private Long[] ranges;
    private List<List<String>> locations;
    IntegralRangePartitionedTableRouter(AbstractVector values, BasicAnyVector locations) {
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

    private static <T extends Comparable> boolean isSorted(List<T> theList) {
        T previous = null;
        for (T t: theList) {
            if (previous != null && t.compareTo(previous) < 0) return false;
            previous = t;
        }
        return true;
    }
    private void initialize(List<Long> longs, BasicAnyVector locations) {
        if (locations.rows() <= 0) {
            throw new RuntimeException("requires at least one location");
        }
        if (locations.getEntity(0).getDataType() != Entity.DATA_TYPE.DT_STRING) {
            throw new RuntimeException("location must be a string");
        }
        if (isSorted(longs) == false) {
            throw new RuntimeException("ranges " + longs.toString() + " not sorted!");
        }
        if (longs.size() != locations.rows() + 1) {
            throw new RuntimeException("expect # locations == # range values - 1");
        }
        ranges = longs.toArray(new Long[longs.size()]);
        this.locations = new ArrayList<>();
        boolean isScalar = locations.getEntity(0).getDataForm() == Entity.DATA_FORM.DF_SCALAR;
        if (isScalar) {
            for (int i = 0; i < locations.rows(); ++i) {
                BasicString location = (BasicString)locations.get(i);
                this.locations.add(Arrays.asList(location.getString()));
            }
        } else {
            for (int i = 0; i < locations.rows(); ++i) {
                BasicStringVector locationVector = (BasicStringVector) locations.getEntity(i);
                this.locations.add(new ArrayList<>());
                for (int j = 0; j < locationVector.rows(); ++j) {
                    this.locations.get(this.locations.size() - 1).add(((BasicString)locationVector.get(j)).getString());
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
            int pos = lowerBound(this.ranges, longVal);
            if (0 <= pos && pos < ranges.length) {
                if (ranges[pos] == longVal)
                    return this.locations.get(pos).get(0);
                else
                    return this.locations.get(pos - 1).get(0);
            }
            throw new RuntimeException(partitionColumn.getNumber().longValue() + " not in partitioning range!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
