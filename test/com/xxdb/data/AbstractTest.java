package com.xxdb.data;

import com.xxdb.io.BigEndianDataInputStream;
import com.xxdb.io.BigEndianDataOutputStream;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class AbstractTest{
    @Test
    public void test_AbstractEntity(){
        class BasicEntity extends AbstractEntity{
            private Entity.DATA_FORM dataForm;
            public BasicEntity(Entity.DATA_FORM data_form){
                this.dataForm = data_form;
            }
            @Override
            public Entity.DATA_FORM getDataForm() {
                return dataForm;
            }
        }
        BasicEntity be = new BasicEntity(Entity.DATA_FORM.DF_PAIR);
        assertTrue(be.isPair());
        assertEquals(Entity.DATA_CATEGORY.LOGICAL,be.getDataCategory(Entity.DATA_TYPE.DT_BOOL));
        assertEquals(Entity.DATA_CATEGORY.LITERAL,be.getDataCategory(Entity.DATA_TYPE.DT_STRING));
        assertEquals(Entity.DATA_CATEGORY.LITERAL,be.getDataCategory(Entity.DATA_TYPE.DT_SYMBOL));
        assertEquals(Entity.DATA_CATEGORY.FLOATING,be.getDataCategory(Entity.DATA_TYPE.DT_DOUBLE));
        assertEquals(Entity.DATA_CATEGORY.FLOATING,be.getDataCategory(Entity.DATA_TYPE.DT_FLOAT));
        assertEquals(Entity.DATA_CATEGORY.INTEGRAL,be.getDataCategory(Entity.DATA_TYPE.DT_BYTE));
        assertEquals(Entity.DATA_CATEGORY.INTEGRAL,be.getDataCategory(Entity.DATA_TYPE.DT_SHORT));
        assertEquals(Entity.DATA_CATEGORY.INTEGRAL,be.getDataCategory(Entity.DATA_TYPE.DT_INT));
        assertEquals(Entity.DATA_CATEGORY.INTEGRAL,be.getDataCategory(Entity.DATA_TYPE.DT_LONG));
        assertEquals(Entity.DATA_CATEGORY.SYSTEM,be.getDataCategory(Entity.DATA_TYPE.DT_FUNCTIONDEF));
        assertEquals(Entity.DATA_CATEGORY.SYSTEM,be.getDataCategory(Entity.DATA_TYPE.DT_HANDLE));
        assertEquals(Entity.DATA_CATEGORY.NOTHING,be.getDataCategory(Entity.DATA_TYPE.DT_VOID));
        assertEquals(Entity.DATA_CATEGORY.MIXED,be.getDataCategory(Entity.DATA_TYPE.DT_ANY));
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,be.getDataCategory(Entity.DATA_TYPE.DT_TIMESTAMP));
        assertEquals(Entity.DATA_CATEGORY.TEMPORAL,be.getDataCategory(Entity.DATA_TYPE.DT_IPADDR));
        assertTrue(new BasicEntity(Entity.DATA_FORM.DF_DICTIONARY).isDictionary());
        assertTrue(new BasicEntity(Entity.DATA_FORM.DF_CHUNK).isChunk());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_AbstractMatrix(){
        class BasicMatrix extends AbstractMatrix{

            protected BasicMatrix(int rows, int columns) {
                super(rows, columns);
            }

            @Override
            protected void readMatrixFromInputStream(int rows, int columns, ExtendedDataInput in) throws IOException {

            }

            @Override
            protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {

            }

            @Override
            public DATA_CATEGORY getDataCategory() {
                return null;
            }

            @Override
            public DATA_TYPE getDataType() {
                return null;
            }

            @Override
            public boolean isNull(int row, int column) {
                return false;
            }

            @Override
            public void setNull(int row, int column) {

            }

            @Override
            public Scalar get(int row, int column) {
                return null;
            }

            @Override
            public Class<?> getElementClass() {
                return null;
            }
        }
        BasicStringVector bsv = new BasicStringVector(new String[]{"MSFT","GOOG","META"});
        BasicMatrix bm = new BasicMatrix(3,3);
        bm.setRowLabels(bsv);
        assertEquals("[MSFT,GOOG,META]",bm.getRowLabels().getString());
        BasicSymbolVector bsyv = new BasicSymbolVector(3);
        bsyv.setString(0,"a");
        bsyv.setString(1,"b");
        bsyv.setString(2,"c");
        bm.setColumnLabels(bsyv);
        assertEquals("[a,b,c]",bm.getColumnLabels().getString());
        bm.setRowLabels(new BasicStringVector(new String[]{"HW"}));
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_AbstractMatrix_setColumns(){
        class BasicMatrix extends AbstractMatrix{

            protected BasicMatrix(int rows, int columns) {
                super(rows, columns);
            }

            @Override
            protected void readMatrixFromInputStream(int rows, int columns, ExtendedDataInput in) throws IOException {

            }

            @Override
            protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {

            }

            @Override
            public DATA_CATEGORY getDataCategory() {
                return null;
            }

            @Override
            public DATA_TYPE getDataType() {
                return null;
            }

            @Override
            public boolean isNull(int row, int column) {
                return false;
            }

            @Override
            public void setNull(int row, int column) {

            }

            @Override
            public Scalar get(int row, int column) {
                return null;
            }

            @Override
            public Class<?> getElementClass() {
                return null;
            }
        }
        BasicMatrix bm = new BasicMatrix(1,1);
        bm.setColumnLabels(new BasicStringVector(new String[]{"XM","OP"}));
    }

    @Test
    public void test_AbstractMatix_getString(){
        class BasicMatrix extends AbstractMatrix{
            private int[] values;
            protected BasicMatrix(int rows, int columns) {
                super(rows, columns);
                values = new int[rows*columns];
            }

            @Override
            protected void readMatrixFromInputStream(int rows, int columns, ExtendedDataInput in) throws IOException {

            }

            @Override
            protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {

            }

            @Override
            public DATA_CATEGORY getDataCategory() {
                return null;
            }

            @Override
            public DATA_TYPE getDataType() {
                return null;
            }

            @Override
            public boolean isNull(int row, int column) {
                return false;
            }

            @Override
            public void setNull(int row, int column) {

            }

            @Override
            public Scalar get(int row, int column) {
                return new BasicInt(values[getIndex(row,column)]);
            }

            @Override
            public Class<?> getElementClass() {
                return null;
            }

            public void setValues(int row,int column,int value){
                values[getIndex(row,column)] = value;
            }
        }
        BasicMatrix bm = new BasicMatrix(2,2);
        bm.setRowLabels(new BasicStringVector(new String[]{"OP","VO"}));
        bm.setColumnLabels(new BasicIntVector(new int[]{10,20}));
        bm.setValues(0,0,1);
        bm.setValues(0,1,2);
        bm.setValues(1,0,3);
        bm.setValues(1,1,4);
        assertEquals("   10 20\n" +
                "OP 1  2 \n" +
                "VO 3  4 \n",bm.getString().toString());
        System.out.println(Utils.DISPLAY_ROWS);
    }

    @Test(expected = RuntimeException.class)
    public void test_AbstractVector_deserialize() throws IOException {
        class BasicVector extends AbstractVector{


            public BasicVector(DATA_FORM df) {
                super(df);
            }

            @Override
            protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {

            }

            @Override
            public Vector combine(Vector vector) {
                return null;
            }

            @Override
            public Vector getSubVector(int[] indices) {
                return null;
            }

            @Override
            public int asof(Scalar value) {
                return 0;
            }

            @Override
            public boolean isNull(int index) {
                return false;
            }

            @Override
            public void setNull(int index) {

            }

            @Override
            public Scalar get(int index) {
                return null;
            }

            @Override
            public void set(int index, Entity value) throws Exception {

            }

            @Override
            public Class<?> getElementClass() {
                return null;
            }

            @Override
            public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {

            }

            @Override
            public int getUnitLength() {
                return 0;
            }

            @Override
            public void Append(Scalar value) throws Exception {

            }

            @Override
            public void Append(Vector value) throws Exception {

            }

            @Override
            public DATA_CATEGORY getDataCategory() {
                return null;
            }

            @Override
            public DATA_TYPE getDataType() {
                return null;
            }

            @Override
            public int rows() {
                return 0;
            }
        }
        BasicVector bv = new BasicVector(Entity.DATA_FORM.DF_VECTOR);
        ExtendedDataInput in = new BigEndianDataInputStream(new InputStream() {
            @Override
            public int read() throws IOException {
                return 0;
            }
        });
        bv.deserialize(0,1,in);
    }

    @Test(expected = RuntimeException.class)
    public void test_abstractVector_serialize() throws IOException {
        class BasicVector extends AbstractVector{


            public BasicVector(DATA_FORM df) {
                super(df);
            }

            @Override
            protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {

            }

            @Override
            public Vector combine(Vector vector) {
                return null;
            }

            @Override
            public Vector getSubVector(int[] indices) {
                return null;
            }

            @Override
            public int asof(Scalar value) {
                return 0;
            }

            @Override
            public boolean isNull(int index) {
                return false;
            }

            @Override
            public void setNull(int index) {

            }

            @Override
            public Scalar get(int index) {
                return null;
            }

            @Override
            public void set(int index, Entity value) throws Exception {

            }

            @Override
            public Class<?> getElementClass() {
                return null;
            }

            @Override
            public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {

            }

            @Override
            public int getUnitLength() {
                return 0;
            }

            @Override
            public void Append(Scalar value) throws Exception {

            }

            @Override
            public void Append(Vector value) throws Exception {

            }

            @Override
            public DATA_CATEGORY getDataCategory() {
                return null;
            }

            @Override
            public DATA_TYPE getDataType() {
                return null;
            }

            @Override
            public int rows() {
                return 0;
            }
        }
        BasicVector bv = new BasicVector(Entity.DATA_FORM.DF_VECTOR);
        AbstractVector.NumElementAndPartial numElementAndPartial = new AbstractVector.NumElementAndPartial(10,2);
        bv.serialize(0,0,1,numElementAndPartial, ByteBuffer.allocate(8));
    }

    @Test(expected = RuntimeException.class)
    public void test_AbstractVector_write() throws IOException {
        class BasicVector extends AbstractVector{


            public BasicVector(DATA_FORM df) {
                super(df);
            }

            @Override
            protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {

            }

            @Override
            public Vector combine(Vector vector) {
                return null;
            }

            @Override
            public Vector getSubVector(int[] indices) {
                return null;
            }

            @Override
            public int asof(Scalar value) {
                return 0;
            }

            @Override
            public boolean isNull(int index) {
                return false;
            }

            @Override
            public void setNull(int index) {

            }

            @Override
            public Scalar get(int index) {
                return null;
            }

            @Override
            public void set(int index, Entity value) throws Exception {

            }

            @Override
            public Class<?> getElementClass() {
                return null;
            }

            @Override
            public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {

            }

            @Override
            public int getUnitLength() {
                return 0;
            }

            @Override
            public void Append(Scalar value) throws Exception {

            }

            @Override
            public void Append(Vector value) throws Exception {

            }

            @Override
            public DATA_CATEGORY getDataCategory() {
                return null;
            }

            @Override
            public DATA_TYPE getDataType() {
                return null;
            }

            @Override
            public int rows() {
                return 0;
            }
        }
        BasicVector bv = new BasicVector(Entity.DATA_FORM.DF_VECTOR);
        bv.writeVectorToBuffer(ByteBuffer.allocate(8));
    }

    @Test(expected = RuntimeException.class)
    public void test_AbstractVector_CompressedMethod(){
        class BasicVector extends AbstractVector{


            public BasicVector(DATA_FORM df) {
                super(df);
            }

            @Override
            protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {

            }

            @Override
            public Vector combine(Vector vector) {
                return null;
            }

            @Override
            public Vector getSubVector(int[] indices) {
                return null;
            }

            @Override
            public int asof(Scalar value) {
                return 0;
            }

            @Override
            public boolean isNull(int index) {
                return false;
            }

            @Override
            public void setNull(int index) {

            }

            @Override
            public Scalar get(int index) {
                return null;
            }

            @Override
            public void set(int index, Entity value) throws Exception {

            }

            @Override
            public Class<?> getElementClass() {
                return null;
            }

            @Override
            public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {

            }

            @Override
            public int getUnitLength() {
                return 0;
            }

            @Override
            public void Append(Scalar value) throws Exception {

            }

            @Override
            public void Append(Vector value) throws Exception {

            }

            @Override
            public DATA_CATEGORY getDataCategory() {
                return null;
            }

            @Override
            public DATA_TYPE getDataType() {
                return null;
            }

            @Override
            public int rows() {
                return 0;
            }
        }
        BasicVector bv = new BasicVector(Entity.DATA_FORM.DF_VECTOR);
        bv.setCompressedMethod(Vector.COMPRESS_LZ4);
        bv.setCompressedMethod(3);
    }

    @Test(expected = RuntimeException.class)
    public void test_AbstractVector_other() throws IOException {
        class BasicVector extends AbstractVector{


            public BasicVector(DATA_FORM df) {
                super(df);
            }

            @Override
            protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException {

            }

            @Override
            public Vector combine(Vector vector) {
                return null;
            }

            @Override
            public Vector getSubVector(int[] indices) {
                return null;
            }

            @Override
            public int asof(Scalar value) {
                return 0;
            }

            @Override
            public boolean isNull(int index) {
                return false;
            }

            @Override
            public void setNull(int index) {

            }

            @Override
            public Scalar get(int index) {
                return null;
            }

            @Override
            public void set(int index, Entity value) throws Exception {

            }

            @Override
            public Class<?> getElementClass() {
                return null;
            }

            @Override
            public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {

            }

            @Override
            public int getUnitLength() {
                return 0;
            }

            @Override
            public void Append(Scalar value) throws Exception {

            }

            @Override
            public void Append(Vector value) throws Exception {

            }

            @Override
            public DATA_CATEGORY getDataCategory() {
                return null;
            }

            @Override
            public DATA_TYPE getDataType() {
                return DATA_TYPE.DT_TIMESTAMP;
            }

            @Override
            public int rows() {
                return 0;
            }
        }
        BasicVector bv = new BasicVector(Entity.DATA_FORM.DF_VECTOR);
        ExtendedDataOutput output = new BigEndianDataOutputStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                System.out.println(b);
            }
        });
        bv.writeCompressed(output);
        assertEquals(16,BasicVector.getUnitLength(Entity.DATA_TYPE.DT_POINT));
        BasicVector.getUnitLength(Entity.DATA_TYPE.DT_ANY);
    }

    @Test
    public void test_AbstractVector_CheckCompressedMethod(){
        assertFalse(AbstractVector.checkCompressedMethod(Entity.DATA_TYPE.DT_POINT_ARRAY,Vector.COMPRESS_DELTA));
        assertTrue(AbstractVector.checkCompressedMethod(Entity.DATA_TYPE.DT_ANY,Vector.COMPRESS_LZ4));
        assertTrue(AbstractVector.checkCompressedMethod(Entity.DATA_TYPE.DT_INT,Vector.COMPRESS_DELTA));
        assertFalse(AbstractVector.checkCompressedMethod(Entity.DATA_TYPE.DT_SYMBOL,2));
    }

    @Test
    public void test_BasicAbstractScalar_getScale(){
        AbstractScalar as = new BasicInt(3);
        assertEquals(0,as.getScale());
    }

    @Test(expected = RuntimeException.class)
    public void test_AbstractVector_getJsonString(){
        AbstractVector av = new BasicIntVector(new int[]{1,2,3});
        av.getJsonString(1);
    }

    @Test(expected = RuntimeException.class)
    public void test_AbstractVector_getExtraParamForType(){
        AbstractVector av = new BasicDateVector(new int[]{15,22,37});
        av.getExtraParamForType();
    }

    @Test
    public void test_AbstractVector_writeCompressed() throws Exception {
        AbstractVector av = new BasicDecimal32Vector(2,2);
        av.set(0,new BasicDecimal32(11,2));
        av.set(1,new BasicDecimal32(24,2));
        av.writeCompressed(new BigEndianDataOutputStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                System.out.println(b);
            }
        }));
    }
}