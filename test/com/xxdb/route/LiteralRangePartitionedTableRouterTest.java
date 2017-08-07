package com.xxdb.route;

import com.xxdb.data.*;

import java.util.Arrays;

public class LiteralRangePartitionedTableRouterTest {
    public static void main(String args[]) {
        {
            BasicStringVector schema = new BasicStringVector(Arrays.asList("A", "B","ZZA", "ZZZ"));
            BasicAnyVector locations = new BasicAnyVector(3);
            locations.setEntity(0, new BasicStringVector(Arrays.asList("192.168.1.25:8847:active-A", "192.168.1.25:8848:active-backup-A")));
            locations.setEntity(1, new BasicStringVector(Arrays.asList("192.168.1.27:8847:active-C", "192.168.1.27:8848:active-backup-C")));
            locations.setEntity(2, new BasicStringVector(Arrays.asList("192.168.1.27:8847:active-B", "192.168.1.27:8848:active-backup-B")));
            LiteralRangePartitionedTableRouter router = new LiteralRangePartitionedTableRouter(schema, locations);

            assert router.route(new BasicString("A")).equals("192.168.1.25:8847:active-A");
            assert router.route(new BasicString("AA")).equals("192.168.1.25:8847:active-A");
            assert router.route(new BasicString("B")).equals("192.168.1.27:8847:active-C");
            assert router.route(new BasicString("BCB")).equals("192.168.1.27:8847:active-C");
            assert router.route(new BasicString("ZZB")).equals("192.168.1.27:8847:active-B");
            try {
                router.route(new BasicString("ZZZ"));
            } catch (RuntimeException e) {
                assert(true);
            } catch (Exception e) {
                assert(false);
            }
        }
    }
}
