package com.xxdb.route;

abstract class AbstractRangePartitionedTableRouter implements TableRouter {
    /**
     * java equivalent of c++ lower_bound.
     * @param A list to be searched on
     * @param x element for which is being searched
     * @param <T> types that implements Comparable<T> interface
     * @return index of the first element that is no less than x, or -1 if:
     *           1) list is empty
     *           2) list has only one element
     *           3) x < list.get(0) or x >= list[list.size() - 1]
     *
     */
    protected  <T extends Comparable<T> > int lowerBound(T[] A, T x) {
        if (A.length == 0 || A.length == 1 || x.compareTo(A[0]) < 0 || x.compareTo(A[A.length - 1]) >= 0)
            return -1;
        int l = 0, h = A.length;
        while (l < h) {
            int m = l + ((h - l) >> 1);
            if (A[m].compareTo(x) < 0) { // A[m] < x
                l = m + 1;
            } else { // A[m] >= x
                h = m;
            }
        }
        return l;
    }
}
