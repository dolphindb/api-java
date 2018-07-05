package com.xxdb.route;

import com.xxdb.data.Scalar;

interface TableRouter {
    // given a partition column value, return "host:port:alias"
    String route(Scalar partitionColumn);
}
