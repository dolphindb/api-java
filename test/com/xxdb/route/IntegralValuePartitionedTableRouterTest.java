package com.xxdb.route;

import com.xxdb.data.*;

import java.util.Arrays;

public class IntegralValuePartitionedTableRouterTest {
    public static void main(String args[]) {
        {
            BasicLongVector schema = new BasicLongVector(Arrays.asList(0l, 1l, 2l, 3l, 4l, 5l, 6l, 7l, 8l, 9l, 10l));
            BasicAnyVector locations = new BasicAnyVector(11);
            locations.setEntity(0, new BasicStringVector(Arrays.asList("192.168.1.25:8847:active-A", "192.168.1.25:8848:active-backup-A")));
            locations.setEntity(1, new BasicStringVector(Arrays.asList("192.168.1.27:8847:active-B", "192.168.1.27:8848:active-backup-B")));
            locations.setEntity(2, new BasicStringVector(Arrays.asList("192.168.1.25:8847:active-A", "192.168.1.25:8848:active-backup-A")));            locations.setEntity(3, new BasicStringVector(Arrays.asList("192.168.1.27:8847:active-B", "192.168.1.27:8848:active-backup-B")));
            locations.setEntity(3, new BasicStringVector(Arrays.asList("192.168.1.27:8847:active-B", "192.168.1.27:8848:active-backup-B")));
            locations.setEntity(4, new BasicStringVector(Arrays.asList("192.168.1.25:8847:active-A", "192.168.1.25:8848:active-backup-A")));            locations.setEntity(3, new BasicStringVector(Arrays.asList("192.168.1.27:8847:active-B", "192.168.1.27:8848:active-backup-B")));
            locations.setEntity(5, new BasicStringVector(Arrays.asList("192.168.1.27:8847:active-B", "192.168.1.27:8848:active-backup-B")));
            locations.setEntity(6, new BasicStringVector(Arrays.asList("192.168.1.25:8847:active-A", "192.168.1.25:8848:active-backup-A")));            locations.setEntity(3, new BasicStringVector(Arrays.asList("192.168.1.27:8847:active-B", "192.168.1.27:8848:active-backup-B")));
            locations.setEntity(7, new BasicStringVector(Arrays.asList("192.168.1.27:8847:active-B", "192.168.1.27:8848:active-backup-B")));
            locations.setEntity(8, new BasicStringVector(Arrays.asList("192.168.1.25:8847:active-A", "192.168.1.25:8848:active-backup-A")));            locations.setEntity(3, new BasicStringVector(Arrays.asList("192.168.1.27:8847:active-B", "192.168.1.27:8848:active-backup-B")));
            locations.setEntity(9, new BasicStringVector(Arrays.asList("192.168.1.27:8847:active-B", "192.168.1.27:8848:active-backup-B")));
            locations.setEntity(10, new BasicStringVector(Arrays.asList("192.168.1.25:8847:active-A", "192.168.1.25:8848:active-backup-A")));            locations.setEntity(3, new BasicStringVector(Arrays.asList("192.168.1.27:8847:active-B", "192.168.1.27:8848:active-backup-B")));
            IntegralValuePartitionedTableRouter router = new IntegralValuePartitionedTableRouter(schema, locations);

            assert router.route(new BasicInt(0)).equals("192.168.1.25:8847:active-A");
            assert router.route(new BasicInt(1)).equals("192.168.1.27:8847:active-B");
            assert router.route(new BasicInt(2)).equals("192.168.1.25:8847:active-A");
            assert router.route(new BasicInt(9)).equals("192.168.1.27:8847:active-B");
            try {
                router.route(new BasicInt(11));
            } catch (RuntimeException e) {
                assert(true);
            } catch (Exception e) {
                assert(false);
            }
        }

        {
            BasicLongVector schema = new BasicLongVector(Arrays.asList(0l, 1l, 2l, 3l, 4l, 5l, 6l, 7l, 8l, 9l, 10l));
            BasicAnyVector locations = new BasicAnyVector(11);
            locations.setEntity(0, new BasicString("192.168.1.25:8847:active-A"));
            locations.setEntity(1, new BasicString("192.168.1.27:8847:active-B"));
            locations.setEntity(2, new BasicString("192.168.1.25:8847:active-A"));
            locations.setEntity(3, new BasicString("192.168.1.27:8847:active-B"));
            locations.setEntity(4, new BasicString("192.168.1.25:8847:active-A"));
            locations.setEntity(5, new BasicString("192.168.1.27:8847:active-B"));
            locations.setEntity(6, new BasicString("192.168.1.25:8847:active-A"));
            locations.setEntity(7, new BasicString("192.168.1.27:8847:active-B"));
            locations.setEntity(8, new BasicString("192.168.1.25:8847:active-A"));
            locations.setEntity(9, new BasicString("192.168.1.27:8847:active-B"));
            locations.setEntity(10, new BasicString("192.168.1.25:8847:active-A"));
            IntegralValuePartitionedTableRouter router = new IntegralValuePartitionedTableRouter(schema, locations);

            assert router.route(new BasicInt(0)).equals("192.168.1.25:8847:active-A");
            assert router.route(new BasicInt(1)).equals("192.168.1.27:8847:active-B");
            assert router.route(new BasicInt(2)).equals("192.168.1.25:8847:active-A");
            assert router.route(new BasicInt(9)).equals("192.168.1.27:8847:active-B");
            try {
                router.route(new BasicInt(11));
            } catch (RuntimeException e) {
                assert(true);
            } catch (Exception e) {
                assert(false);
            }
        }
    }
}
