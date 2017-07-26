package com.xxdb.streaming.data;

import java.io.IOException;
import java.lang.*;

import com.xxdb.streaming.io.ExtendedDataInput;

public class BasicEntityFactory implements EntityFactory{
	private TypeFactory[] factories;
	
	public BasicEntityFactory(){
		factories = new TypeFactory[Entity.DATA_TYPE.values().length];
		factories[Entity.DATA_TYPE.DT_BOOL.ordinal()] = new BooleanFactory();
		factories[Entity.DATA_TYPE.DT_BYTE.ordinal()] = new ByteFactory();
		factories[Entity.DATA_TYPE.DT_SHORT.ordinal()] = new ShortFactory();
		factories[Entity.DATA_TYPE.DT_INT.ordinal()] = new IntFactory();
		factories[Entity.DATA_TYPE.DT_LONG.ordinal()] = new LongFactory();
		factories[Entity.DATA_TYPE.DT_FLOAT.ordinal()] = new FloatFactory();
		factories[Entity.DATA_TYPE.DT_DOUBLE.ordinal()] = new DoubleFactory();
		factories[Entity.DATA_TYPE.DT_MINUTE.ordinal()] = new MinuteFactory();
		factories[Entity.DATA_TYPE.DT_SECOND.ordinal()] = new SecondFactory();
		factories[Entity.DATA_TYPE.DT_TIME.ordinal()] = new TimeFactory();
		factories[Entity.DATA_TYPE.DT_DATE.ordinal()] = new DateFactory();
		factories[Entity.DATA_TYPE.DT_MONTH.ordinal()] = new MonthFactory();
		factories[Entity.DATA_TYPE.DT_DATETIME.ordinal()] = new DateTimeFactory();
		factories[Entity.DATA_TYPE.DT_TIMESTAMP.ordinal()] = new TimestampFactory();
		factories[Entity.DATA_TYPE.DT_SYMBOL.ordinal()] = new SymbolFactory();
		factories[Entity.DATA_TYPE.DT_STRING.ordinal()] = new StringFactory();
		factories[Entity.DATA_TYPE.DT_FUNCTIONDEF.ordinal()] = new FunctionDefFactory();
		factories[Entity.DATA_TYPE.DT_HANDLE.ordinal()] = new SystemHandleFactory();
		factories[Entity.DATA_TYPE.DT_CODE.ordinal()] = new MetaCodeFactory();
		factories[Entity.DATA_TYPE.DT_DATASOURCE.ordinal()] = new DataSourceFactory();
		factories[Entity.DATA_TYPE.DT_RESOURCE.ordinal()] = new DataSourceFactory();
	}
	
	@Override
	public Entity createEntity(Entity.DATA_FORM form, Entity.DATA_TYPE type, ExtendedDataInput in) throws IOException{
		if(form == Entity.DATA_FORM.DF_TABLE)
			return new BasicTable(in);
		else if(form == Entity.DATA_FORM.DF_CHART)
			return new BasicChart(in);
		else if(form == Entity.DATA_FORM.DF_DICTIONARY)
			return new BasicDictionary(type, in);
		else if(form == Entity.DATA_FORM.DF_SET)
			return new BasicSet(type, in);
		else if(type == Entity.DATA_TYPE.DT_ANY && form == Entity.DATA_FORM.DF_VECTOR)
			return new BasicAnyVector(in);
		else if(type == Entity.DATA_TYPE.DT_VOID && form == Entity.DATA_FORM.DF_SCALAR){
			in.readBoolean();
			return new Void();
		}
		else{
			int index = type.ordinal();
			if(factories[index] == null)
				throw new IOException("Data type " + type.name() +" is not supported yet.");
			else if(form == Entity.DATA_FORM.DF_VECTOR)
				return factories[index].createVector(in);
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
		int index = type.ordinal();
		if(factories[index] == null)
			return null;
		else
			return factories[index].createMatrixWithDefaultValue(rows, columns);
	}

	@Override
	public Vector createVectorWithDefaultValue(Entity.DATA_TYPE type, int size) {
		int index = type.ordinal();
		if(factories[index] == null)
			return null;
		else
			return factories[index].createVectorWithDefaultValue(size);
	}
	
	@Override
	public Vector createPairWithDefaultValue(Entity.DATA_TYPE type) {
		int index = type.ordinal();
		if(factories[index] == null)
			return null;
		else
			return factories[index].createPairWithDefaultValue();
	}

	@Override
	public Scalar createScalarWithDefaultValue(Entity.DATA_TYPE type) {
		int index = type.ordinal();
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
	
	private class StringFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicString(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicStringVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicStringVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicStringMatrix(in);}
		public Scalar createScalarWithDefaultValue() { return new BasicString("");}
		public Vector createVectorWithDefaultValue(int size){ return new BasicStringVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicStringVector(Entity.DATA_FORM.DF_PAIR, 2, false);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicStringMatrix(rows, columns);}
	}
	
	private class SymbolFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicString(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicStringVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicStringVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicStringMatrix(in);}
		public Scalar createScalarWithDefaultValue() { return new BasicString("");}
		public Vector createVectorWithDefaultValue(int size){ return new BasicStringVector(Entity.DATA_FORM.DF_VECTOR, size, true);}
		public Vector createPairWithDefaultValue(){ return new BasicStringVector(Entity.DATA_FORM.DF_PAIR, 2, true);}
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
	

}
