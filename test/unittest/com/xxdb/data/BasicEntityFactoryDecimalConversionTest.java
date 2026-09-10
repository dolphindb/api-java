package com.xxdb.data;

import org.junit.Test;

import java.math.BigDecimal;

import static com.xxdb.data.Entity.DATA_TYPE.DT_DECIMAL128;
import static com.xxdb.data.Entity.DATA_TYPE.DT_DECIMAL32;
import static com.xxdb.data.Entity.DATA_TYPE.DT_DECIMAL64;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BasicEntityFactoryDecimalConversionTest {

    @Test
    public void createScalarConvertsLongToDecimal128() throws Exception {
        Entity entity = BasicEntityFactory.createScalar(DT_DECIMAL128, 1000L, 6);

        assertTrue(entity instanceof BasicDecimal128);
        assertEquals("1000.000000", entity.getString());
    }

    @Test
    public void createScalarConvertsNegativeLongToDecimal128() throws Exception {
        Entity entity = BasicEntityFactory.createScalar(DT_DECIMAL128, -2L, 6);

        assertTrue(entity instanceof BasicDecimal128);
        assertEquals("-2.000000", entity.getString());
    }

    @Test
    public void createScalarConvertsBigDecimalToDecimal128WithoutDoubleRounding() throws Exception {
        Entity entity = BasicEntityFactory.createScalar(DT_DECIMAL128, new BigDecimal("12345678901234567890.123456"), 6);

        assertTrue(entity instanceof BasicDecimal128);
        assertEquals("12345678901234567890.123456", entity.getString());
    }

    @Test
    public void createScalarConvertsBigDecimalToAllDecimalWidths() throws Exception {
        assertEquals("12.3400", BasicEntityFactory.createScalar(DT_DECIMAL32, new BigDecimal("12.34"), 4).getString());
        assertEquals("12.340000", BasicEntityFactory.createScalar(DT_DECIMAL64, new BigDecimal("12.34"), 6).getString());
        assertEquals("12.34000000", BasicEntityFactory.createScalar(DT_DECIMAL128, new BigDecimal("12.34"), 8).getString());
    }

    @Test
    public void createScalarKeepsExistingLongToDecimal64Behavior() throws Exception {
        Entity entity = BasicEntityFactory.createScalar(DT_DECIMAL64, 1000L, 6);

        assertTrue(entity instanceof BasicDecimal64);
        assertEquals("1000.000000", entity.getString());
    }

    @Test
    public void createScalarKeepsExistingLongToDecimal32OverflowBehavior() throws Exception {
        try {
            BasicEntityFactory.createScalar(DT_DECIMAL32, Long.MAX_VALUE, 6);
            fail("Expected long overflow for DT_DECIMAL32");
        } catch (RuntimeException e) {
            assertEquals("Failed to insert data, long cannot be converted because it exceeds the range of DT_DECIMAL32.", e.getMessage());
        }
    }
}
