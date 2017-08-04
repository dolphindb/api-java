package com.xxdb.route;

import com.xxdb.data.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class LiteralRangePartitionedTableRouter extends AbstractRangePartitionedTableRouter {
    private String[] ranges;
    private List<List<String>> locations;
    LiteralRangePartitionedTableRouter(AbstractVector values, BasicAnyVector locations) {
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

    private static <T extends Comparable> boolean isSorted(List<T> theList) {
        T previous = null;
        for (T t: theList) {
            if (previous != null && t.compareTo(previous) < 0) return false;
            previous = t;
        }
        return true;
    }
    private void initialize(List<String> strings, BasicAnyVector locations) {
        if (locations.rows() <= 0) {
            throw new RuntimeException("requires at least one location");
        }
        if (locations.getEntity(0).getDataType() != Entity.DATA_TYPE.DT_STRING) {
            throw new RuntimeException("location must be a string");
        }
        if (isSorted(strings) == false) {
            throw new RuntimeException("ranges " + strings.toString() + " not sorted!");
        }
        if (strings.size() != locations.rows() + 1) {
            throw new RuntimeException("expect # locations == # range values - 1");
        }
        ranges = strings.toArray(new String[strings.size()]);
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
        if (partitionColumn.getDataCategory() != Entity.DATA_CATEGORY.LITERAL)
            throw  new RuntimeException("invalid column category " + partitionColumn.getDataCategory().name() + ", expect Literal category.");
        try {
            String stringVal = partitionColumn.getString();
            if (stringVal.compareTo("ACMR") > 0) {
                System.out.println();
            }
            int pos = lowerBound(this.ranges, stringVal);
            if (0 <= pos && pos < ranges.length) {
                if (ranges[pos].equals(stringVal))
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
