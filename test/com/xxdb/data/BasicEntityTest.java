package com.xxdb.data;

import com.xxdb.io.BigEndianDataOutputStream;
import com.xxdb.io.ExtendedDataOutput;
import org.junit.Assert;
import org.junit.Test;

import javax.management.RuntimeErrorException;

import static org.junit.Assert.*;
import java.io.IOException;
import java.io.OutputStream;

public class BasicEntityTest {
    @Test
    public void test_typeToCategory(){
        assertEquals(Entity.DATA_CATEGORY.LOGICAL,Entity.typeToCategory(Entity.DATA_TYPE.DT_BOOL));
        assertEquals(Entity.DATA_CATEGORY.BINARY,Entity.typeToCategory(Entity.DATA_TYPE.DT_INT128));
        assertEquals(Entity.DATA_CATEGORY.BINARY,Entity.typeToCategory(Entity.DATA_TYPE.DT_UUID));
        assertEquals(Entity.DATA_CATEGORY.BINARY,Entity.typeToCategory(Entity.DATA_TYPE.DT_IPADDR));
        assertEquals(Entity.DATA_CATEGORY.MIXED,Entity.typeToCategory(Entity.DATA_TYPE.DT_ANY));
        assertEquals(Entity.DATA_CATEGORY.NOTHING,Entity.typeToCategory(Entity.DATA_TYPE.DT_VOID));
        assertEquals(Entity.DATA_CATEGORY.SYSTEM,Entity.typeToCategory(Entity.DATA_TYPE.DT_DURATION));
        assertEquals(Entity.DATA_CATEGORY.FLOATING,Entity.typeToCategory(Entity.DATA_TYPE.DT_FLOAT));
        assertEquals(Entity.DATA_CATEGORY.FLOATING,Entity.typeToCategory(Entity.DATA_TYPE.DT_DOUBLE));
    }

    @Test(expected = RuntimeException.class)
    public void test_Entity_valueOf(){
        Entity.DATA_TYPE.valueOf(100);
    }

    @Test(expected = RuntimeException.class)
    public void test_Entity_valueOfTypeName(){
        Entity.DATA_TYPE.valueOfTypeName("POINT");
        Entity.DATA_TYPE.valueOfTypeName("VECTOR");
    }

    @Test(expected = IOException.class)
    public void test_writeCompressed() throws IOException {
        Entity entity = new BasicInt(4);
        entity.writeCompressed(new BigEndianDataOutputStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                System.out.println(b);
            }
        }));
    }
}
