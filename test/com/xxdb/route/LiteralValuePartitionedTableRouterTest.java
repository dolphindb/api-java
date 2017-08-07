package com.xxdb.route;

import com.xxdb.data.*;

import java.util.Arrays;

public class LiteralValuePartitionedTableRouterTest {
    public static void main(String args[]) {
        {
            BasicStringVector schema = new BasicStringVector(Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"));
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
            LiteralValuePartitionedTableRouter router = new LiteralValuePartitionedTableRouter(schema, locations);

            assert router.route(new BasicString("0")).equals("192.168.1.25:8847:active-A");
            assert router.route(new BasicString("1")).equals("192.168.1.27:8847:active-B");
            assert router.route(new BasicString("2")).equals("192.168.1.25:8847:active-A");
            assert router.route(new BasicString("9")).equals("192.168.1.27:8847:active-B");
            try {
                router.route(new BasicString("11"));
            } catch (RuntimeException e) {
                assert(true);
            } catch (Exception e) {
                assert(false);
            }
        }

        {
            BasicStringVector schema = new BasicStringVector(Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"));
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
            LiteralValuePartitionedTableRouter router = new LiteralValuePartitionedTableRouter(schema, locations);

            assert router.route(new BasicString("0")).equals("192.168.1.25:8847:active-A");
            assert router.route(new BasicString("1")).equals("192.168.1.27:8847:active-B");
            assert router.route(new BasicString("2")).equals("192.168.1.25:8847:active-A");
            assert router.route(new BasicString("9")).equals("192.168.1.27:8847:active-B");
            try {
                router.route(new BasicString("11"));
            } catch (RuntimeException e) {
                assert(true);
            } catch (Exception e) {
                assert(false);
            }
        }
    }
}
