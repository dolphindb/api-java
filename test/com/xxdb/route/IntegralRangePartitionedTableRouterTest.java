package com.xxdb.route;

import com.xxdb.data.*;

import java.util.Arrays;

public class IntegralRangePartitionedTableRouterTest {
    public static void main(String args[]) {
        {
            BasicLongVector schema = new BasicLongVector(Arrays.asList(0l, 1l,5l,10l));
            BasicAnyVector locations = new BasicAnyVector(3);
            locations.setEntity(0, new BasicStringVector(Arrays.asList("192.168.1.25:8847:active-A", "192.168.1.25:8848:active-backup-A")));
            locations.setEntity(1, new BasicStringVector(Arrays.asList("192.168.1.27:8847:active-C", "192.168.1.27:8848:active-backup-C")));
            locations.setEntity(2, new BasicStringVector(Arrays.asList("192.168.1.27:8847:active-B", "192.168.1.27:8848:active-backup-B")));
            IntegralRangePartitionedTableRouter router = new IntegralRangePartitionedTableRouter(schema, locations);

            assert router.route(new BasicInt(0)).equals("192.168.1.25:8847:active-A");
            assert router.route(new BasicInt(1)).equals("192.168.1.27:8847:active-C");
            assert router.route(new BasicInt(2)).equals("192.168.1.27:8847:active-C");
            assert router.route(new BasicInt(3)).equals("192.168.1.27:8847:active-C");
            assert router.route(new BasicInt(4)).equals("192.168.1.27:8847:active-C");
            assert router.route(new BasicInt(5)).equals("192.168.1.27:8847:active-B");
            assert router.route(new BasicInt(8)).equals("192.168.1.27:8847:active-B");
            assert router.route(new BasicInt(9)).equals("192.168.1.27:8847:active-B");
            try {
                router.route(new BasicInt(-1));
            } catch (RuntimeException e) {
                assert(true);
            } catch (Exception e) {
                assert(false);
            }
            try {
                router.route(new BasicInt(10));
            } catch (RuntimeException e) {
                assert(true);
            } catch (Exception e) {
                assert(false);
            }
        }

        {
            BasicLongVector schema = new BasicLongVector(Arrays.asList(0l,5l,10l));
            BasicAnyVector locations = new BasicAnyVector(2);
            locations.setEntity(0, new BasicString("192.168.1.25:8847:active-A"));
            locations.setEntity(1, new BasicString("192.168.1.27:8847:active-B"));
            IntegralRangePartitionedTableRouter router = new IntegralRangePartitionedTableRouter(schema, locations);

            assert router.route(new BasicInt(0)).equals("192.168.1.25:8847:active-A");
            assert router.route(new BasicInt(2)).equals("192.168.1.25:8847:active-A");
            assert router.route(new BasicInt(5)).equals("192.168.1.27:8847:active-B");
            assert router.route(new BasicInt(8)).equals("192.168.1.27:8847:active-B");
            try {
                router.route(new BasicInt(-1));
            } catch (RuntimeException e) {
                assert(true);
            } catch (Exception e) {
                assert(false);
            }
            try {
                router.route(new BasicInt(10));
            } catch (RuntimeException e) {
                assert(true);
            } catch (Exception e) {
                assert(false);
            }
        }
    }
}
