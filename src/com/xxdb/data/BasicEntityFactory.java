package com.xxdb.data;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.xxdb.data.Entity.DATA_TYPE;
import com.xxdb.data.Entity.DURATION;
import com.xxdb.io.ExtendedDataInput;

import static com.xxdb.data.Entity.DATA_TYPE.DT_LONG;
import static com.xxdb.data.Entity.DATA_TYPE.DT_STRING;

public class BasicEntityFactory implements EntityFactory{
	private TypeFactory[] factories;
	private TypeFactory[] factoriesExt;
	private static EntityFactory factory = new BasicEntityFactory();
	
	public static EntityFactory instance(){
		return factory;
	}

	public BasicEntityFactory(){
		int typeCount = DATA_TYPE.DT_OBJECT.getValue() + 1;
		factories = new TypeFactory[typeCount];
		factoriesExt = new TypeFactory[typeCount];
		factories[Entity.DATA_TYPE.DT_BOOL.getValue()] = new BooleanFactory();
		factories[Entity.DATA_TYPE.DT_BYTE.getValue()] = new ByteFactory();
		factories[Entity.DATA_TYPE.DT_SHORT.getValue()] = new ShortFactory();
		factories[Entity.DATA_TYPE.DT_INT.getValue()] = new IntFactory();
		factories[DT_LONG.getValue()] = new LongFactory();
		factories[Entity.DATA_TYPE.DT_FLOAT.getValue()] = new FloatFactory();
		factories[Entity.DATA_TYPE.DT_DOUBLE.getValue()] = new DoubleFactory();
		factories[Entity.DATA_TYPE.DT_MINUTE.getValue()] = new MinuteFactory();
		factories[Entity.DATA_TYPE.DT_SECOND.getValue()] = new SecondFactory();
		factories[Entity.DATA_TYPE.DT_TIME.getValue()] = new TimeFactory();
		factories[Entity.DATA_TYPE.DT_NANOTIME.getValue()] = new NanoTimeFactory();
		factories[Entity.DATA_TYPE.DT_DATE.getValue()] = new DateFactory();
		factories[Entity.DATA_TYPE.DT_DATEHOUR.getValue()] = new DateHourFactory();
		factories[Entity.DATA_TYPE.DT_MONTH.getValue()] = new MonthFactory();
		factories[Entity.DATA_TYPE.DT_DATETIME.getValue()] = new DateTimeFactory();
		factories[Entity.DATA_TYPE.DT_TIMESTAMP.getValue()] = new TimestampFactory();
		factories[Entity.DATA_TYPE.DT_NANOTIMESTAMP.getValue()] = new NanoTimestampFactory();
		factories[Entity.DATA_TYPE.DT_SYMBOL.getValue()] = new SymbolFactory();
		factories[Entity.DATA_TYPE.DT_STRING.getValue()] = new StringFactory();
		factories[Entity.DATA_TYPE.DT_BLOB.getValue()] = new BlobFactory();
		factories[Entity.DATA_TYPE.DT_FUNCTIONDEF.getValue()] = new FunctionDefFactory();
		factories[Entity.DATA_TYPE.DT_HANDLE.getValue()] = new SystemHandleFactory();
		factories[Entity.DATA_TYPE.DT_CODE.getValue()] = new MetaCodeFactory();
		factories[Entity.DATA_TYPE.DT_DATASOURCE.getValue()] = new DataSourceFactory();
		factories[Entity.DATA_TYPE.DT_RESOURCE.getValue()] = new ResourceFactory();
		factories[Entity.DATA_TYPE.DT_COMPRESS.getValue()] = new CompressFactory();
		factories[Entity.DATA_TYPE.DT_UUID.getValue()] = new UuidFactory();
		factories[Entity.DATA_TYPE.DT_INT128.getValue()] = new Int128Factory();
		factories[Entity.DATA_TYPE.DT_IPADDR.getValue()] = new IPAddrFactory();
		factories[Entity.DATA_TYPE.DT_COMPLEX.getValue()] = new ComplexFactory();
		factories[Entity.DATA_TYPE.DT_POINT.getValue()] = new PointFactory();
		factories[Entity.DATA_TYPE.DT_DURATION.getValue()] = new DurationFactory();
		factoriesExt[Entity.DATA_TYPE.DT_SYMBOL.getValue()] = new ExtendedSymbolFactory();
	}
	
	@Override
	public Entity createEntity(Entity.DATA_FORM form, Entity.DATA_TYPE type, ExtendedDataInput in, boolean extended) throws IOException{
		if(form == Entity.DATA_FORM.DF_TABLE)
			return new BasicTable(in);
		else if(form == Entity.DATA_FORM.DF_CHART)
			return new BasicChart(in);
		else if(form == Entity.DATA_FORM.DF_DICTIONARY)
			return new BasicDictionary(type, in);
		else if(form == Entity.DATA_FORM.DF_SET)
			return new BasicSet(type, in);
		else if(form == Entity.DATA_FORM.DF_CHUNK)
			return new BasicChunkMeta(in);
		else if(type == Entity.DATA_TYPE.DT_ANY && (form == Entity.DATA_FORM.DF_VECTOR || form == Entity.DATA_FORM.DF_PAIR))
			return new BasicAnyVector(in);
		else if(type.getValue() >= Entity.DATA_TYPE.DT_BOOL_ARRAY.getValue() && type.getValue() <= Entity.DATA_TYPE.DT_POINT_ARRAY.getValue())
			return new BasicArrayVector(type, in);
		else if(type == Entity.DATA_TYPE.DT_VOID && form == Entity.DATA_FORM.DF_SCALAR){
			in.readBoolean();
			return new Void();
		}
		else{
			int index = type.getValue();
			if(factories[index] == null)
				throw new IOException("Data type " + type.name() +" is not supported yet.");
			else if(form == Entity.DATA_FORM.DF_VECTOR){
				if(!extended)
					return factories[index].createVector(in);
				else
					return factoriesExt[index].createVector(in);
			}
			else if(form == Entity.DATA_FORM.DF_SCALAR)
				return factories[index].createScalar(in);
			else if(form == Entity.DATA_FORM.DF_MATRIX)
				return factories[index].createMatrix(in);
			else if(form == Entity.DATA_FORM.DF_PAIR)
				return factories[index].createPair(in);
			else
				throw new IOException("Data form " + form.name() +" is not supported yet.");
		}
	}

	@Override
	public Matrix createMatrixWithDefaultValue(Entity.DATA_TYPE type, int rows, int columns) {
		int index = type.getValue();
		if(factories[index] == null)
			return null;
		else
			return factories[index].createMatrixWithDefaultValue(rows, columns);
	}

	@Override
	public Vector createVectorWithDefaultValue(Entity.DATA_TYPE type, int size) {
		int index = type.getValue();
		if(factories[index] == null)
			return null;
		else
			return factories[index].createVectorWithDefaultValue(size);
	}
	
	@Override
	public Vector createPairWithDefaultValue(Entity.DATA_TYPE type) {
		int index = type.getValue();
		if(factories[index] == null)
			return null;
		else
			return factories[index].createPairWithDefaultValue();
	}

	@Override
	public Scalar createScalarWithDefaultValue(Entity.DATA_TYPE type) {
		int index = type.getValue();
		if(factories[index] == null)
			return null;
		else
			return factories[index].createScalarWithDefaultValue();
	}
	
	private interface TypeFactory{
		Scalar createScalar(ExtendedDataInput in) throws IOException;
		Vector createVector(ExtendedDataInput in) throws IOException;
		Vector createPair(ExtendedDataInput in) throws IOException;
		Matrix createMatrix(ExtendedDataInput in) throws IOException;
		Scalar createScalarWithDefaultValue();
		Vector createVectorWithDefaultValue(int size);
		Vector createPairWithDefaultValue();
		Matrix createMatrixWithDefaultValue(int rows, int columns);
	}
	
	private class BooleanFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicBoolean(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicBooleanVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicBooleanVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicBooleanMatrix(in);}
		public Scalar createScalarWithDefaultValue() { return new BasicBoolean(false);}
		public Vector createVectorWithDefaultValue(int size){ return new BasicBooleanVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicBooleanVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicBooleanMatrix(rows, columns);}
	}
	
	private class ByteFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicByte(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicByteVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicByteVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicByteMatrix(in);}
		public Scalar createScalarWithDefaultValue() { return new BasicByte((byte)0);}
		public Vector createVectorWithDefaultValue(int size){ return new BasicByteVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicByteVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicByteMatrix(rows, columns);}
	}
	
	private class ShortFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicShort(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicShortVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicShortVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicShortMatrix(in);}
		public Scalar createScalarWithDefaultValue() { return new BasicShort((short)0);}
		public Vector createVectorWithDefaultValue(int size){ return new BasicShortVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicShortVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicShortMatrix(rows, columns);}
	}
	
	private class IntFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicInt(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicIntVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicIntVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicIntMatrix(in);}
		public Scalar createScalarWithDefaultValue() { return new BasicInt(0);}
		public Vector createVectorWithDefaultValue(int size){ return new BasicIntVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicIntVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicIntMatrix(rows, columns);}
	}
	
	private class LongFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicLong(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicLongVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicLongVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicLongMatrix(in);}
		public Scalar createScalarWithDefaultValue() { return new BasicLong(0);}
		public Vector createVectorWithDefaultValue(int size){ return new BasicLongVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicLongVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicLongMatrix(rows, columns);}
	}
	
	private class FloatFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicFloat(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicFloatVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicFloatVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicFloatMatrix(in);}
		public Scalar createScalarWithDefaultValue() { return new BasicFloat(0);}
		public Vector createVectorWithDefaultValue(int size){ return new BasicFloatVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicFloatVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicFloatMatrix(rows, columns);}
	}
	
	private class DoubleFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicDouble(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicDoubleVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicDoubleVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicDoubleMatrix(in);}
		public Scalar createScalarWithDefaultValue() { return new BasicDouble(0);}
		public Vector createVectorWithDefaultValue(int size){ return new BasicDoubleVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicDoubleVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicDoubleMatrix(rows, columns);}
	}
	
	private class MinuteFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicMinute(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicMinuteVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicMinuteVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicMinuteMatrix(in);}
		public Scalar createScalarWithDefaultValue() { return new BasicMinute(0);}
		public Vector createVectorWithDefaultValue(int size){ return new BasicMinuteVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicMinuteVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicMinuteMatrix(rows, columns);}
	}
	
	private class SecondFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicSecond(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicSecondVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicSecondVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicSecondMatrix(in);}
		public Scalar createScalarWithDefaultValue() { return new BasicInt(0);}
		public Vector createVectorWithDefaultValue(int size){ return new BasicSecondVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicSecondVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicSecondMatrix(rows, columns);}
	}
	
	private class TimeFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicTime(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicTimeVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicTimeVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicTimeMatrix(in);}
		public Scalar createScalarWithDefaultValue() { return new BasicTime(0);}
		public Vector createVectorWithDefaultValue(int size){ return new BasicTimeVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicTimeVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicTimeMatrix(rows, columns);}
	}
	private class NanoTimeFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicNanoTime(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicNanoTimeVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicNanoTimeVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicNanoTimeMatrix(in);}
		public Scalar createScalarWithDefaultValue() { return new BasicNanoTime(0);}
		public Vector createVectorWithDefaultValue(int size){ return new BasicNanoTimeVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicNanoTimeVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicNanoTimeMatrix(rows, columns);}
	}

	private class DateFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicDate(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicDateVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicDateVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicDateMatrix(in);}
		public Scalar createScalarWithDefaultValue() { return new BasicDate(0);}
		public Vector createVectorWithDefaultValue(int size){ return new BasicDateVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicDateVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicDateMatrix(rows, columns);}
	}
	
	private class DateHourFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicDateHour(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicDateHourVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicDateHourVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicDateHourMatrix(in);}
		public Scalar createScalarWithDefaultValue() { return new BasicDateHour(0);}
		public Vector createVectorWithDefaultValue(int size) { return new BasicDateHourVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicDateHourVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicDateHourMatrix(rows, columns);}
	}
	
	private class MonthFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicMonth(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicMonthVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicMonthVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicMonthMatrix(in);}
		public Scalar createScalarWithDefaultValue() { return new BasicMonth(0);}
		public Vector createVectorWithDefaultValue(int size){ return new BasicMonthVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicMonthVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicMonthMatrix(rows, columns);}
	}
	
	private class DateTimeFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicDateTime(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicDateTimeVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicDateTimeVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicDateTimeMatrix(in);}
		public Scalar createScalarWithDefaultValue() { return new BasicDateTime(0);}
		public Vector createVectorWithDefaultValue(int size){ return new BasicDateTimeVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicDateTimeVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicDateTimeMatrix(rows, columns);}
	}
	
	private class TimestampFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicTimestamp(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicTimestampVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicTimestampVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicTimestampMatrix(in);}
		public Scalar createScalarWithDefaultValue() { return new BasicTimestamp(0);}
		public Vector createVectorWithDefaultValue(int size){ return new BasicTimestampVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicTimestampVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicTimestampMatrix(rows, columns);}
	}
	private class NanoTimestampFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicNanoTimestamp(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicNanoTimestampVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicNanoTimestampVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicNanoTimestampMatrix(in);}
		public Scalar createScalarWithDefaultValue() { return new BasicNanoTimestamp(0);}
		public Vector createVectorWithDefaultValue(int size){ return new BasicNanoTimestampVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicNanoTimestampVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicNanoTimestampMatrix(rows, columns);}
	}
	
	private class Int128Factory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicInt128(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicInt128Vector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicInt128Vector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { throw new RuntimeException("Matrix for INT128 not supported yet.");}
		public Scalar createScalarWithDefaultValue() { return new BasicInt128(0, 0);}
		public Vector createVectorWithDefaultValue(int size){ return new BasicInt128Vector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicInt128Vector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ throw new RuntimeException("Matrix for INT128 not supported yet.");}
	}
	
	private class UuidFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicUuid(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicUuidVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicUuidVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { throw new RuntimeException("Matrix for UUID not supported yet.");}
		public Scalar createScalarWithDefaultValue() { return new BasicUuid(0, 0);}
		public Vector createVectorWithDefaultValue(int size){ return new BasicUuidVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicUuidVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ throw new RuntimeException("Matrix for UUID not supported yet.");}
	}
	
	private class IPAddrFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicIPAddr(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicIPAddrVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicIPAddrVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { throw new RuntimeException("Matrix for IPADDR not supported yet.");}
		public Scalar createScalarWithDefaultValue() { return new BasicIPAddr(0, 0);}
		public Vector createVectorWithDefaultValue(int size){ return new BasicIPAddrVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicIPAddrVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ throw new RuntimeException("Matrix for IPADDR not supported yet.");}
	}
	
	private class ComplexFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicComplex(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicComplexVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicComplexVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicComplexMatrix(in);}
		public Scalar createScalarWithDefaultValue() { return new BasicComplex(0, 0);}
		public Vector createVectorWithDefaultValue(int size){ return new BasicComplexVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicComplexVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicComplexMatrix(rows, columns);}
	}
	
	private class PointFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicPoint(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicPointVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicPointVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { throw new RuntimeException("Matrix for Point not supported yet.");}
		public Scalar createScalarWithDefaultValue() { return new BasicPoint(0, 0);}
		public Vector createVectorWithDefaultValue(int size){ return new BasicPointVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicPointVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ throw new RuntimeException("Matrix for Point not supported yet.");}
	}
	
	private class DurationFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicDuration(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { throw new RuntimeException("Vector for Duration not supported yet.");}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicDurationVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { throw new RuntimeException("Matrix for Duration not supported yet.");}
		public Scalar createScalarWithDefaultValue() { return new BasicDuration(DURATION.NS, 0);}
		public Vector createVectorWithDefaultValue(int size){ throw new RuntimeException("Vector for Duration not supported yet.");}
		public Vector createPairWithDefaultValue(){ return new BasicDurationVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ throw new RuntimeException("Matrix for Duration not supported yet.");}
	}

	private class StringFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicString(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicStringVector(Entity.DATA_FORM.DF_VECTOR, in, false, false);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicStringVector(Entity.DATA_FORM.DF_PAIR, in, false, false);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicStringMatrix(in);}
		public Scalar createScalarWithDefaultValue() { return new BasicString("");}
		public Vector createVectorWithDefaultValue(int size){ return new BasicStringVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicStringVector(Entity.DATA_FORM.DF_PAIR, 2, false);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicStringMatrix(rows, columns);}
	}
	
	private class SymbolFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicString(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicStringVector(Entity.DATA_FORM.DF_VECTOR, in, false, false);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicStringVector(Entity.DATA_FORM.DF_PAIR, in, false, false);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicStringMatrix(in);}
		public Scalar createScalarWithDefaultValue() { return new BasicString("");}
		public Vector createVectorWithDefaultValue(int size){ return new BasicStringVector(Entity.DATA_FORM.DF_VECTOR, size, true);}
		public Vector createPairWithDefaultValue(){ return new BasicStringVector(Entity.DATA_FORM.DF_PAIR, 2, true);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicStringMatrix(rows, columns);}
	}
	
	private class ExtendedSymbolFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicString(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicSymbolVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicSymbolVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicStringMatrix(in);}
		public Scalar createScalarWithDefaultValue() { return new BasicString("");}
		public Vector createVectorWithDefaultValue(int size){ return new BasicSymbolVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicStringVector(Entity.DATA_FORM.DF_PAIR, 2, true);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicStringMatrix(rows, columns);}
	}

	private class BlobFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicString(in, true);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicStringVector(Entity.DATA_FORM.DF_VECTOR, in, false, true);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicStringVector(Entity.DATA_FORM.DF_PAIR, in, false, true);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicStringMatrix(in);}
		public Scalar createScalarWithDefaultValue() { return new BasicString("");}
		public Vector createVectorWithDefaultValue(int size){ return new BasicStringVector(Entity.DATA_FORM.DF_VECTOR, size, false,true);}
		public Vector createPairWithDefaultValue(){ return new BasicStringVector(Entity.DATA_FORM.DF_PAIR, 2, false,true);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicStringMatrix(rows, columns);}
	}

	private class FunctionDefFactory extends StringFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicSystemEntity(in, Entity.DATA_TYPE.DT_FUNCTIONDEF);}
	}
	
	private class MetaCodeFactory extends StringFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicSystemEntity(in, Entity.DATA_TYPE.DT_CODE);}
	}
	
	private class DataSourceFactory extends StringFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicSystemEntity(in, Entity.DATA_TYPE.DT_DATASOURCE);}
	}
	
	private class SystemHandleFactory extends StringFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicSystemEntity(in, Entity.DATA_TYPE.DT_HANDLE);}
	}

	private class ResourceFactory extends StringFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicSystemEntity(in, Entity.DATA_TYPE.DT_RESOURCE);}
	}

	private class CompressFactory extends StringFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicSystemEntity(in, Entity.DATA_TYPE.DT_COMPRESS);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicByteVector(Entity.DATA_FORM.DF_VECTOR, in);}
	}
	public static Scalar createScalar(DATA_TYPE dataType, Object object){
		if(object==null) {
			Scalar scalar = BasicEntityFactory.instance().createScalarWithDefaultValue(dataType);
			scalar.setNull();
			return scalar;
		}
		if(object instanceof Boolean) {
			return createScalar(dataType,((Boolean)object).booleanValue());
		}
		if(object instanceof Byte) {
			return createScalar(dataType,((Byte)object).byteValue());
		}
		if(object instanceof Character){
			return createScalar(dataType,((Character)object).charValue());
		}
		if(object instanceof Short){
			return createScalar(dataType,((Short)object).shortValue());
		}
		if(object instanceof Integer) {
			return createScalar(dataType,((Integer)object).intValue());
		}
		if(object instanceof Long) {
			return createScalar(dataType,((Long)object).longValue());
		}
		if(object instanceof Double) {
			return createScalar(dataType,((Double)object).doubleValue());
		}
		if(object instanceof Float) {
			return createScalar(dataType,((Float)object).floatValue());
		}
		if(object instanceof String) {
			return createScalar(dataType,(String)object);
		}
		if(object instanceof Entity){
			return createScalar(dataType,(Entity)object);
		}
		if(object instanceof Scalar){
			return createScalar(dataType,(Scalar)object);
		}
		throw new RuntimeException("Failed to insert data, invalid data type for "+dataType);
	}

	private static Scalar createScalar(DATA_TYPE dataType, Entity val) {
		if(val.isScalar()&&val.getDataType()==dataType) {
			return (Scalar) val;
		}else{
			throw new RuntimeException("Failed to insert data, invalid data type for "+dataType);
		}
	}
	private static Scalar createScalar(DATA_TYPE dataType, Scalar val) {
		if(val.getDataType()==dataType)
			return val;
		else{
			throw new RuntimeException("Failed to insert data, invalid data type for "+dataType);
		}
	}
	private static Scalar createScalar(DATA_TYPE dataType, boolean val) {
		switch (dataType) {
			case DT_BOOL:
				return new BasicBoolean(val);
			default:
				throw new RuntimeException("Failed to insert data, unsupported data type.");
		}
	}
	private static Scalar createScalar(DATA_TYPE dataType, char val) {
		return createScalar(dataType,(byte)val);
	}
	private static Scalar createScalar(DATA_TYPE dataType, byte val) {
		switch (dataType) {
			case DT_BYTE:
				return new BasicByte(val);
			default:
				throw new RuntimeException("Failed to insert data, unsupported data type.");
		}
	}
	private static Scalar createScalar(DATA_TYPE dataType, short val) {
		switch (dataType) {
			case DT_SHORT:
				return new BasicShort(val);
			default:
				throw new RuntimeException("Failed to insert data, unsupported data type.");
		}
	}
	private static Scalar createScalar(DATA_TYPE dataType, char[] val) {
		return createScalar(dataType, new String(val));
	}
	private static boolean isScalarValid(DATA_TYPE scalarType,DATA_TYPE expectedType){
		if(scalarType==expectedType)
			return true;
		if(scalarType==DT_STRING){
			if(expectedType==DATA_TYPE.DT_SYMBOL||expectedType==DATA_TYPE.DT_SYMBOL)
				return true;
		}
		return false;
	}
	private static Scalar createScalar(DATA_TYPE dataType, String val) {
		switch (dataType) {
			case DT_INT128: {
				return BasicInt128.fromString(val);
			}
			case DT_UUID: {
				return BasicUuid.fromString(val);
			}
			case DT_IPADDR: {
				return BasicIPAddr.fromString(val);
			}
			case DT_SYMBOL:
			case DT_BLOB: {
				return new BasicString(val);
			}
			case DT_STRING:
				return new BasicString(val);
			default:
				throw new RuntimeException("Failed to insert data, unsupported data type " + dataType);
		}
	}
	private static Scalar createScalar(DATA_TYPE dataType, float val) {
		switch (dataType) {
			case DT_FLOAT:
				return new BasicFloat(val);
			default:
				throw new RuntimeException("Failed to insert data, unsupported data type.");
		}
	}
	private static Scalar createScalar(DATA_TYPE dataType, double val) {
		switch (dataType) {
			case DT_DOUBLE:
				return new BasicDouble(val);
			default:
				throw new RuntimeException("Failed to insert data, unsupported data type.");
		}
	}
	private static Scalar createScalar(DATA_TYPE dataType, int val) {
		switch (dataType) {
			case DT_INT:
				return new BasicInt(val);
			case DT_DATE:
				return new BasicDate(val);
			case DT_MONTH:
				return new BasicMonth(val);
			case DT_TIME:
				return new BasicTime(val);
			case DT_SECOND:
				return new BasicSecond(val);
			case DT_MINUTE:
				return new BasicMinute(val);
			case DT_DATETIME:
				return new BasicDateTime(val);
			case DT_DATEHOUR:
				return new BasicDateHour(val);
			default:
				throw new RuntimeException("Failed to insert data, unsupported data type.");
		}
	}
	private static Scalar createScalar(DATA_TYPE dataType, long val){
		switch (dataType) {
			case DT_LONG:
				return new BasicLong(val);
			case DT_NANOTIME:
				return new BasicNanoTime(val);
			case DT_NANOTIMESTAMP:
				return new BasicNanoTimestamp(val);
			case DT_TIMESTAMP:
				return new BasicTimestamp(val);
			default:
				throw new RuntimeException("Failed to insert data, unsupported data type.");
		}
	}
}
