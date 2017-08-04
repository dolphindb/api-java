package com.xxdb.route;

import com.xxdb.data.*;

import java.util.Arrays;

public class LiteralListPartitionedTableRouterTest {
    public static void main(String args[]) {
        {
            BasicAnyVector schema = new BasicAnyVector(2);
            schema.setEntity(0, new BasicStringVector(Arrays.asList("1", "3", "5", "7", "9")));
            schema.setEntity(1, new BasicStringVector(Arrays.asList("2", "4", "6", "8", "10")));
            BasicAnyVector locations = new BasicAnyVector(2);
            locations.setEntity(0, new BasicStringVector(Arrays.asList("192.168.1.25:8847:active-A", "192.168.1.25:8848:active-backup-A")));
            locations.setEntity(1, new BasicStringVector(Arrays.asList("192.168.1.27:8847:active-B", "192.168.1.27:8848:active-backup-B")));
            LiteralListPartitionedTableRouter router = new LiteralListPartitionedTableRouter(schema, locations);

            assert router.route(new BasicString("1")).equals("192.168.1.25:8847:active-A");
            assert router.route(new BasicString("2")).equals("192.168.1.27:8847:active-B");
            try {
                router.route(new BasicString("0"));
            } catch (RuntimeException e) {
                assert(true);
            } catch (Exception e) {
                assert(false);
            }
        }

        {
            BasicAnyVector schema = new BasicAnyVector(2);
            schema.setEntity(0, new BasicStringVector(Arrays.asList("1", "3", "5", "7", "9")));
            schema.setEntity(1, new BasicStringVector(Arrays.asList("2", "4", "6", "8", "10")));
            BasicAnyVector locations = new BasicAnyVector(2);
            locations.setEntity(0, new BasicString("192.168.1.25:8847:active-A"));
            locations.setEntity(1, new BasicString("192.168.1.27:8847:active-B"));
            LiteralListPartitionedTableRouter router = new LiteralListPartitionedTableRouter(schema, locations);
            assert router.route(new BasicString("1")).equals("192.168.1.25:8847:active-A");
            assert router.route(new BasicString("2")).equals("192.168.1.27:8847:active-B");
            try {
                router.route(new BasicString("0"));
            } catch (RuntimeException e) {
                assert(true);
            } catch (Exception e) {
                assert(false);
            }
        }
    }
}
