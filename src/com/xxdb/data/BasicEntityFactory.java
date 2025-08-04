package com.xxdb.data;

import com.xxdb.data.Entity.DATA_TYPE;
import com.xxdb.data.Entity.DURATION;
import com.xxdb.io.ExtendedDataInput;
import java.io.IOException;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.Date;
import static com.xxdb.data.Entity.DATA_TYPE.*;

public class BasicEntityFactory implements EntityFactory{
	private TypeFactory[] factories;
	private TypeFactory[] factoriesExt;
	private static EntityFactory factory = new BasicEntityFactory();
	
	public static EntityFactory instance(){
		return factory;
	}

	public BasicEntityFactory(){
		int typeCount = DT_MKTDATA.getValue() + 1;
		factories = new TypeFactory[typeCount];
		factoriesExt = new TypeFactory[typeCount];
		factories[Entity.DATA_TYPE.DT_BOOL.getValue()] = new BooleanFactory();
		factories[Entity.DATA_TYPE.DT_VOID.getValue()] = new VoidFactory();
		factories[Entity.DATA_TYPE.DT_BYTE.getValue()] = new ByteFactory();
		factories[Entity.DATA_TYPE.DT_SHORT.getValue()] = new ShortFactory();
		factories[Entity.DATA_TYPE.DT_INT.getValue()] = new IntFactory();
		factories[Entity.DATA_TYPE.DT_LONG.getValue()] = new LongFactory();
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
		factories[DT_DECIMAL32.getValue()] = new Decimal32Factory();
		factories[DT_DECIMAL64.getValue()] = new Decimal64Factory();
		factories[DT_DECIMAL128.getValue()] = new Decimal128Factory();
		factories[DT_INSTRUMENT.getValue()] = new InstrumentFactory();
		factories[DT_MKTDATA.getValue()] = new MktDataFactory();
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
		else if (form == Entity.DATA_FORM.DF_TENSOR)
			return new BasicTensor(type, in);
		else if(type == Entity.DATA_TYPE.DT_ANY && (form == Entity.DATA_FORM.DF_VECTOR || form == Entity.DATA_FORM.DF_PAIR))
			return new BasicAnyVector(in);
		else if (type == DT_IOTANY)
			return new BasicIotAnyVector(in);
		else if(type.getValue() >= Entity.DATA_TYPE.DT_BOOL_ARRAY.getValue() && type.getValue() <= DT_DECIMAL128_ARRAY.getValue())
			return new BasicArrayVector(type, in);
		else if(type == Entity.DATA_TYPE.DT_VOID && form == Entity.DATA_FORM.DF_SCALAR){
			in.readBoolean();
			return new Void();
		} else {
			int index = type.getValue();
			if(factories[index] == null) {
				throw new IOException("Data type " + type.name() + " is not supported yet.");
			} else if (form == Entity.DATA_FORM.DF_VECTOR) {
				if(!extended)
					return factories[index].createVector(in);
				else
					return factoriesExt[index].createVector(in);
			} else if (form == Entity.DATA_FORM.DF_SCALAR) {
				return factories[index].createScalar(in);
			} else if (form == Entity.DATA_FORM.DF_MATRIX) {
				return factories[index].createMatrix(in);
			} else if (form == Entity.DATA_FORM.DF_PAIR) {
				return factories[index].createPair(in);
			} else if (form == Entity.DATA_FORM.DF_EXTOBJ) {
				return factories[index].createExtendObj(in);
			} else {
				throw new IOException("Data form " + form.name() +" is not supported yet.");
			}
		}
	}

	@Override
	public Matrix createMatrixWithDefaultValue(Entity.DATA_TYPE type, int rows, int columns) {
		int index = type.getValue();
		if(index >= factories.length || factories[index] == null)
			return null;
		else
			return factories[index].createMatrixWithDefaultValue(rows, columns);
	}

	@Override
	public Vector createVectorWithDefaultValue(Entity.DATA_TYPE type, int size, int extra) {
		int index = type.getValue();
		if(factories[index] == null)
			return null;
		else
			return factories[index].createVectorWithDefaultValue(size, extra);
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
		ExtendObj createExtendObj(ExtendedDataInput in) throws IOException;
		Scalar createScalarWithDefaultValue();
		Vector createVectorWithDefaultValue(int size, int extra);
		Vector createPairWithDefaultValue();
		Matrix createMatrixWithDefaultValue(int rows, int columns);
	}

	private class Decimal64Factory implements TypeFactory{

		@Override
		public Scalar createScalar(ExtendedDataInput in) throws IOException {
			return new BasicDecimal64(in);
		}

		@Override
		public Vector createVector(ExtendedDataInput in) throws IOException {
			return new BasicDecimal64Vector(Entity.DATA_FORM.DF_VECTOR, in, -1);
		}

		@Override
		public Vector createPair(ExtendedDataInput in) throws IOException {
			return new BasicDecimal64Vector(Entity.DATA_FORM.DF_PAIR, in, -1);
		}

		@Override
		public Matrix createMatrix(ExtendedDataInput in) throws IOException {
			return new BasicDecimal64Matrix(in);
		}

		@Override
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException {
			return null;
		}

		@Override
		public Scalar createScalarWithDefaultValue() {
			return new BasicDecimal64((long) 0,0);
		}

		@Override
		public Vector createVectorWithDefaultValue(int size, int extra) {
			return new BasicDecimal64Vector(size, extra);
		}

		@Override
		public Vector createPairWithDefaultValue() {
			return null;
		}

		@Override
		public Matrix createMatrixWithDefaultValue(int rows, int columns) {
			return new BasicDecimal64Matrix(rows, columns, 0);
		}
	}

	private class Decimal128Factory implements TypeFactory {
		@Override
		public Scalar createScalar(ExtendedDataInput in) throws IOException {
			return new BasicDecimal128(in);
		}

		@Override
		public Vector createVector(ExtendedDataInput in) throws IOException {
			return new BasicDecimal128Vector(Entity.DATA_FORM.DF_VECTOR, in, -1);
		}

		@Override
		public Vector createPair(ExtendedDataInput in) throws IOException {
			return new BasicDecimal128Vector(Entity.DATA_FORM.DF_PAIR, in, -1);
		}

		@Override
		public Matrix createMatrix(ExtendedDataInput in) throws IOException {
			return new BasicDecimal128Matrix(in);
		}

		@Override
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException {
			return null;
		}

		@Override
		public Scalar createScalarWithDefaultValue() {
			return new BasicDecimal128(BigInteger.valueOf(0),0);
		}

		@Override
		public Vector createVectorWithDefaultValue(int size, int extra) {
			return new BasicDecimal128Vector(size, extra);
		}

		@Override
		public Vector createPairWithDefaultValue() {
			return null;
		}

		@Override
		public Matrix createMatrixWithDefaultValue(int rows, int columns) {
			return new BasicDecimal128Matrix(rows, columns, 0);
		}

	}

	private class Decimal32Factory implements TypeFactory{

		@Override
		public Scalar createScalar(ExtendedDataInput in) throws IOException {
			return new BasicDecimal32(in);
		}

		@Override
		public Vector createVector(ExtendedDataInput in) throws IOException {
			return new BasicDecimal32Vector(Entity.DATA_FORM.DF_VECTOR, in, -1);
		}

		@Override
		public Vector createPair(ExtendedDataInput in) throws IOException {
			return new BasicDecimal32Vector(Entity.DATA_FORM.DF_PAIR, in, -1);
		}

		@Override
		public Matrix createMatrix(ExtendedDataInput in) throws IOException {
			return new BasicDecimal32Matrix(in);
		}

		@Override
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException {
			return null;
		}

		@Override
		public Scalar createScalarWithDefaultValue() {
			return new BasicDecimal32(0, 0);
		}

		@Override
		public Vector createVectorWithDefaultValue(int size, int extra) {
			return new BasicDecimal32Vector(size, extra);
		}

		@Override
		public Vector createPairWithDefaultValue() {
			return null;
		}

		@Override
		public Matrix createMatrixWithDefaultValue(int rows, int columns) {
			return new BasicDecimal32Matrix(rows, columns, 0);
		}
	}

	private class BooleanFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicBoolean(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicBooleanVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicBooleanVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicBooleanMatrix(in);}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicBoolean(false);}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicBooleanVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicBooleanVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicBooleanMatrix(rows, columns);}
	}

	private class VoidFactory implements TypeFactory {
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new Void();}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicVoidVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicVoidVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { throw new RuntimeException("Matrix for DT_VOID not supported yet."); }
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new Void();}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicVoidVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicVoidVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ throw new RuntimeException("Matrix for DT_VOID not supported yet."); }
	}
	
	private class ByteFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicByte(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicByteVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicByteVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicByteMatrix(in);}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicByte((byte)0);}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicByteVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicByteVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicByteMatrix(rows, columns);}
	}
	
	private class ShortFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicShort(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicShortVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicShortVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicShortMatrix(in);}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicShort((short)0);}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicShortVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicShortVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicShortMatrix(rows, columns);}
	}

	private class IntFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicInt(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicIntVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicIntVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicIntMatrix(in);}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicInt(0);}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicIntVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicIntVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicIntMatrix(rows, columns);}
	}
	
	private class LongFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicLong(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicLongVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicLongVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicLongMatrix(in);}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicLong(0);}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicLongVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicLongVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicLongMatrix(rows, columns);}
	}
	
	private class FloatFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicFloat(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicFloatVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicFloatVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicFloatMatrix(in);}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicFloat(0);}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicFloatVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicFloatVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicFloatMatrix(rows, columns);}
	}
	
	private class DoubleFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicDouble(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicDoubleVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicDoubleVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicDoubleMatrix(in);}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicDouble(0);}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicDoubleVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicDoubleVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicDoubleMatrix(rows, columns);}
	}
	
	private class MinuteFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicMinute(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicMinuteVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicMinuteVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicMinuteMatrix(in);}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicMinute(0);}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicMinuteVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicMinuteVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicMinuteMatrix(rows, columns);}
	}
	
	private class SecondFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicSecond(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicSecondVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicSecondVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicSecondMatrix(in);}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicInt(0);}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicSecondVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicSecondVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicSecondMatrix(rows, columns);}
	}
	
	private class TimeFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicTime(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicTimeVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicTimeVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicTimeMatrix(in);}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicTime(0);}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicTimeVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicTimeVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicTimeMatrix(rows, columns);}
	}
	private class NanoTimeFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicNanoTime(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicNanoTimeVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicNanoTimeVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicNanoTimeMatrix(in);}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicNanoTime(0);}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicNanoTimeVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicNanoTimeVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicNanoTimeMatrix(rows, columns);}
	}

	private class DateFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicDate(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicDateVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicDateVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicDateMatrix(in);}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicDate(0);}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicDateVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicDateVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicDateMatrix(rows, columns);}
	}
	
	private class DateHourFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicDateHour(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicDateHourVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicDateHourVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicDateHourMatrix(in);}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicDateHour(0);}
		public Vector createVectorWithDefaultValue(int size, int extra) { return new BasicDateHourVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicDateHourVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicDateHourMatrix(rows, columns);}
	}
	
	private class MonthFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicMonth(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicMonthVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicMonthVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicMonthMatrix(in);}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicMonth(0);}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicMonthVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicMonthVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicMonthMatrix(rows, columns);}
	}
	
	private class DateTimeFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicDateTime(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicDateTimeVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicDateTimeVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicDateTimeMatrix(in);}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicDateTime(0);}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicDateTimeVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicDateTimeVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicDateTimeMatrix(rows, columns);}
	}
	
	private class TimestampFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicTimestamp(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicTimestampVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicTimestampVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicTimestampMatrix(in);}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicTimestamp(0);}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicTimestampVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicTimestampVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicTimestampMatrix(rows, columns);}
	}
	private class NanoTimestampFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicNanoTimestamp(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicNanoTimestampVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicNanoTimestampVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicNanoTimestampMatrix(in);}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicNanoTimestamp(0);}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicNanoTimestampVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicNanoTimestampVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicNanoTimestampMatrix(rows, columns);}
	}
	
	private class Int128Factory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicInt128(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicInt128Vector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicInt128Vector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { throw new RuntimeException("Matrix for INT128 not supported yet.");}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicInt128(0, 0);}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicInt128Vector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicInt128Vector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ throw new RuntimeException("Matrix for INT128 not supported yet.");}
	}
	
	private class UuidFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicUuid(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicUuidVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicUuidVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { throw new RuntimeException("Matrix for UUID not supported yet.");}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicUuid(0, 0);}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicUuidVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicUuidVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ throw new RuntimeException("Matrix for UUID not supported yet.");}
	}
	
	private class IPAddrFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicIPAddr(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicIPAddrVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicIPAddrVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { throw new RuntimeException("Matrix for IPADDR not supported yet.");}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicIPAddr(0, 0);}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicIPAddrVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicIPAddrVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ throw new RuntimeException("Matrix for IPADDR not supported yet.");}
	}
	
	private class ComplexFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicComplex(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicComplexVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicComplexVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicComplexMatrix(in);}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicComplex(0, 0);}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicComplexVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicComplexVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicComplexMatrix(rows, columns);}
	}
	
	private class PointFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicPoint(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicPointVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicPointVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { throw new RuntimeException("Matrix for Point not supported yet.");}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicPoint(0, 0);}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicPointVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicPointVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ throw new RuntimeException("Matrix for Point not supported yet.");}
	}
	
	private class DurationFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicDuration(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { throw new RuntimeException("Vector for Duration not supported yet.");}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicDurationVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { throw new RuntimeException("Matrix for Duration not supported yet.");}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicDuration(DURATION.NS, 0);}
		public Vector createVectorWithDefaultValue(int size, int extra){ throw new RuntimeException("Vector for Duration not supported yet.");}
		public Vector createPairWithDefaultValue(){ return new BasicDurationVector(Entity.DATA_FORM.DF_PAIR, 2);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ throw new RuntimeException("Matrix for Duration not supported yet.");}
	}

	private class StringFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicString(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicStringVector(Entity.DATA_FORM.DF_VECTOR, in, false, false);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicStringVector(Entity.DATA_FORM.DF_PAIR, in, false, false);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicStringMatrix(in, false);}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicString("");}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicStringVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicStringVector(Entity.DATA_FORM.DF_PAIR, 2, false);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicStringMatrix(rows, columns);}
	}
	
	private class SymbolFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicString(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicStringVector(Entity.DATA_FORM.DF_VECTOR, in, true, false);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicStringVector(Entity.DATA_FORM.DF_PAIR, in, true, false);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicStringMatrix(in, true);}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicString("");}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicStringVector(Entity.DATA_FORM.DF_VECTOR, size, true);}
		public Vector createPairWithDefaultValue(){ return new BasicStringVector(Entity.DATA_FORM.DF_PAIR, 2, true);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicStringMatrix(rows, columns);}
	}
	
	private class ExtendedSymbolFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicString(in);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicSymbolVector(Entity.DATA_FORM.DF_VECTOR, in);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicSymbolVector(Entity.DATA_FORM.DF_PAIR, in);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicStringMatrix(in);}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicString("");}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicSymbolVector(size);}
		public Vector createPairWithDefaultValue(){ return new BasicStringVector(Entity.DATA_FORM.DF_PAIR, 2, true);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicStringMatrix(rows, columns);}
	}

	private class BlobFactory implements TypeFactory{
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return new BasicString(in, true);}
		public Vector createVector(ExtendedDataInput in) throws IOException { return new BasicStringVector(Entity.DATA_FORM.DF_VECTOR, in, false, true);}
		public Vector createPair(ExtendedDataInput in) throws IOException { return new BasicStringVector(Entity.DATA_FORM.DF_PAIR, in, false, true);}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return new BasicStringMatrix(in);}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return null;}
		public Scalar createScalarWithDefaultValue() { return new BasicString("", true);}
		public Vector createVectorWithDefaultValue(int size, int extra){ return new BasicStringVector(Entity.DATA_FORM.DF_VECTOR, size, false,true);}
		public Vector createPairWithDefaultValue(){ return new BasicStringVector(Entity.DATA_FORM.DF_PAIR, 2, false,true);}
		public Matrix createMatrixWithDefaultValue(int rows, int columns){ return new BasicStringMatrix(rows, columns);}
	}

	private class InstrumentFactory implements TypeFactory {
		public Scalar createScalar(ExtendedDataInput in) throws IOException { return null;}
		public Vector createVector(ExtendedDataInput in) throws IOException { return null;}
		public Vector createPair(ExtendedDataInput in) throws IOException { return null;}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException { return null;}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException { return new BasicInstrument(in);}
		public Scalar createScalarWithDefaultValue() { return null;}
		public Vector createVectorWithDefaultValue(int size, int extra) { return null;}
		public Vector createPairWithDefaultValue() { return null;}
		public Matrix createMatrixWithDefaultValue(int rows, int columns) { return null;}
	}

	private class MktDataFactory implements TypeFactory {
		public Scalar createScalar(ExtendedDataInput in) throws IOException {return null;}
		public Vector createVector(ExtendedDataInput in) throws IOException {return null;}
		public Vector createPair(ExtendedDataInput in) throws IOException {return null;}
		public Matrix createMatrix(ExtendedDataInput in) throws IOException {return null;}
		public ExtendObj createExtendObj(ExtendedDataInput in) throws IOException {return new BasicMktData(in);}
		public Scalar createScalarWithDefaultValue() {return null;}
		public Vector createVectorWithDefaultValue(int size, int extra) {return null;}
		public Vector createPairWithDefaultValue() {return null;}
		public Matrix createMatrixWithDefaultValue(int rows, int columns) {return null;}
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
	public static Entity createScalar(DATA_TYPE dataType, Object object, int extraParam) throws Exception{
		if (object == null){
			if (dataType.getValue() < 65){
				Scalar scalar = BasicEntityFactory.instance().createScalarWithDefaultValue(dataType);
				scalar.setNull();
				return scalar;
			}else {
				dataType = Entity.DATA_TYPE.values()[dataType.getValue() - 64];
				return BasicEntityFactory.instance().createVectorWithDefaultValue(dataType, 0, extraParam);
			}
		}

		if (object instanceof Boolean)
			return createScalar(dataType, (boolean) object);
		if (object instanceof Boolean[])
			return createAnyVector(dataType, (Boolean[]) object, extraParam);
		if (object instanceof boolean[])
			return createAnyVector(dataType, (boolean[]) object, extraParam);
		if (object instanceof Byte)
			return createScalar(dataType, (byte) object);
		if (object instanceof Byte[])
			return createAnyVector(dataType, (Byte[]) object, extraParam);
		if (object instanceof byte[])
			return createAnyVector(dataType, (byte[]) object, extraParam);
		if (object instanceof Character)
			return createScalar(dataType, (char) object);
		if (object instanceof Character[])
			return createAnyVector(dataType, (Character[]) object, extraParam);
		if (object instanceof char[])
			return createAnyVector(dataType, (char[]) object, extraParam);
		if (object instanceof Short)
			return createScalar(dataType, (short) object);
		if (object instanceof Short[])
			return createAnyVector(dataType, (Short[]) object, extraParam);
		if (object instanceof short[])
            return createAnyVector(dataType, (short[]) object,extraParam);
		if (object instanceof Integer)
			return createScalar(dataType, (int) object, extraParam);
		if (object instanceof Integer[])
			return createAnyVector(dataType, (Integer[]) object, extraParam);
		if (object instanceof int[])
            return createAnyVector(dataType, (int[]) object, extraParam);
		if (object instanceof Long)
			return createScalar(dataType, (long) object, extraParam);
		if (object instanceof Long[])
			return createAnyVector(dataType, (Long[]) object, extraParam);
		if (object instanceof long[])
			return createAnyVector(dataType, (long[]) object, extraParam);
		if (object instanceof Double)
			return createScalar(dataType, (double) object, extraParam);
		if (object instanceof Double[])
			return createAnyVector(dataType, (Double[]) object, extraParam);
		if (object instanceof double[])
			return createAnyVector(dataType, (double[]) object, extraParam);
		if (object instanceof Float)
			return createScalar(dataType, (float) object, extraParam);
		if (object instanceof Float[])
			return createAnyVector(dataType, (Float[]) object, extraParam);
		if (object instanceof float[])
			return createAnyVector(dataType, (float[]) object, extraParam);
		if (object instanceof String)
			return createScalar(dataType, (String) object, extraParam);
		if (object instanceof String[])
			return createAnyVector(dataType, (String[]) object, extraParam);
		if (object instanceof LocalTime)
			return createScalar(dataType, (LocalTime) object);
		if (object instanceof LocalTime[])
			return createAnyVector(dataType, (LocalTime[]) object, extraParam);
		if (object instanceof LocalDate)
			return createScalar(dataType, (LocalDate) object);
		if (object instanceof LocalDate[])
			return createAnyVector(dataType, (LocalDate[]) object, extraParam);
		if (object instanceof LocalDateTime)
			return createScalar(dataType, (LocalDateTime) object);
		if (object instanceof LocalDateTime[])
			return createAnyVector(dataType, (LocalDateTime[]) object, extraParam);
		if (object instanceof Date)
			return createScalar(dataType, (Date) object);
		if (object instanceof Date[])
			return createAnyVector(dataType, (Date[]) object, extraParam);
		if (object instanceof Calendar)
			return createScalar(dataType, (Calendar) object);
		if (object instanceof Calendar[])
			return createAnyVector(dataType, (Calendar[]) object, extraParam);
		if (object instanceof Entity)
			return createScalar(dataType, (Entity) object);
		if (object instanceof Entity[])
			return createAnyVector(dataType, (Entity[]) object, extraParam);

		throw new RuntimeException("Failed to insert data. invalid data type for "+dataType + ".");
	}

    private static boolean checkVectorDataTypeIsRight(DATA_TYPE dataType, DATA_TYPE comparedDataType){
        if (dataType.getValue() < 64)
            throw new RuntimeException("Failed to insert data, only arrayVector support data vector for "+dataType + ".");

        return dataType.equals(comparedDataType);
    }

	private static <T> Vector createAnyVector(DATA_TYPE dataType, T[] val, int extraParam) throws Exception{
		if (dataType.getValue() < 64)
			throw new RuntimeException("Failed to insert data, only arrayVector support data vector for "+dataType + ".");

		dataType = Entity.DATA_TYPE.values()[dataType.getValue() - 64];
		int count = val.length;
		Vector vector = BasicEntityFactory.instance().createVectorWithDefaultValue(dataType, count, extraParam);

		for (int i = 0; i < count; ++i) {
			Entity t = createScalar(dataType, val[i], extraParam);
			vector.set(i, (Scalar) t);
		}

		return vector;
	}

	private static Vector createAnyVector(DATA_TYPE dataType, float[] val, int extraParam) throws Exception{
		if (checkVectorDataTypeIsRight(dataType, DT_FLOAT_ARRAY))
			return new BasicFloatVector(val);

		dataType = Entity.DATA_TYPE.values()[dataType.getValue() - 64];
		int count = val.length;
		Vector vector = BasicEntityFactory.instance().createVectorWithDefaultValue(dataType, count, extraParam);
		for (int i = 0; i < count; ++i) {
			Scalar t = createScalar(dataType, val[i], extraParam);
			vector.set(i, t);
		}

		return vector;
	}

	private static Vector createAnyVector(DATA_TYPE dataType, double[] val, int extraParam) throws Exception{
		if (checkVectorDataTypeIsRight(dataType, DT_DOUBLE_ARRAY))
			return new BasicDoubleVector(val);

		dataType = Entity.DATA_TYPE.values()[dataType.getValue() - 64];
		int count = val.length;
		Vector vector = BasicEntityFactory.instance().createVectorWithDefaultValue(dataType, count, extraParam);
		for (int i = 0; i < count; ++i) {
			Scalar t = createScalar(dataType, val[i], extraParam);
			vector.set(i, t);
		}

		return vector;
	}

	private static Vector createAnyVector(DATA_TYPE dataType, int[] val, int extraParam) throws Exception{
		if (checkVectorDataTypeIsRight(dataType, DT_INT_ARRAY))
			return new BasicIntVector(val);

		dataType = Entity.DATA_TYPE.values()[dataType.getValue() - 64];
		int count = val.length;
		Vector vector = BasicEntityFactory.instance().createVectorWithDefaultValue(dataType, count, extraParam);

		for (int i = 0; i < count; ++i) {
			Scalar t = createScalar(dataType, val[i], extraParam);
			vector.set(i, t);
		}

		return vector;
	}

	private static Vector createAnyVector(DATA_TYPE dataType, short[] val, int extraParam) throws Exception{
		if (checkVectorDataTypeIsRight(dataType, DT_SHORT_ARRAY))
			return new BasicShortVector(val);

		dataType = Entity.DATA_TYPE.values()[dataType.getValue() - 64];
		int count = val.length;
		Vector vector = BasicEntityFactory.instance().createVectorWithDefaultValue(dataType, count, extraParam);

		for (int i = 0; i < count; ++i) {
			Scalar t = createScalar(dataType, val[i]);
			vector.set(i, t);
		}

		return vector;
	}

	private static Vector createAnyVector(DATA_TYPE dataType, byte[] val, int extraParam) throws Exception{
		if (checkVectorDataTypeIsRight(dataType, DT_BYTE_ARRAY))
			return new BasicByteVector(val);

		dataType = Entity.DATA_TYPE.values()[dataType.getValue() - 64];
		int count = val.length;
		Vector vector = BasicEntityFactory.instance().createVectorWithDefaultValue(dataType, count, extraParam);

		for (int i = 0; i < count; ++i) {
			Scalar t = createScalar(dataType, val[i]);
			vector.set(i, t);
		}

		return vector;
	}

	private static Vector createAnyVector(DATA_TYPE dataType, char[] val, int extraParam) throws Exception{
		if (dataType.getValue()<64)
			throw new RuntimeException("Failed to insert data, only arrayVector support data vector for "+dataType + ".");

		dataType = Entity.DATA_TYPE.values()[dataType.getValue() - 64];
		int count = val.length;
		Vector vector = BasicEntityFactory.instance().createVectorWithDefaultValue(dataType, count, extraParam);

		for(int i = 0; i < count; ++i) {
			Scalar t = createScalar(dataType, val[i]);
			vector.set(i, t);
		}

		return vector;
	}

	private static Vector createAnyVector(DATA_TYPE dataType, boolean[] val, int extraParam) throws Exception{
		if (checkVectorDataTypeIsRight(dataType, DT_BOOL_ARRAY)) {
			return new BasicBooleanVector((val));
		}
		dataType = Entity.DATA_TYPE.values()[dataType.getValue() - 64];
		int count = val.length;
		Vector vector = BasicEntityFactory.instance().createVectorWithDefaultValue(dataType, count, extraParam);

		for(int i = 0; i < count; ++i)
		{
			Scalar t = createScalar(dataType, val[i]);
			vector.set(i, t);
		}
		return vector;
	}

    private static Vector createAnyVector(DATA_TYPE dataType, long[] val, int extraParam) throws Exception{
		if (checkVectorDataTypeIsRight(dataType, DT_LONG_ARRAY))
			return new BasicLongVector(val);

		dataType = Entity.DATA_TYPE.values()[dataType.getValue() - 64];
		int count = val.length;
		Vector vector = BasicEntityFactory.instance().createVectorWithDefaultValue(dataType, count, extraParam);
		for (int i = 0; i < count; ++i) {
			Scalar t = createScalar(dataType, val[i], extraParam);
			vector.set(i, t);
		}

		return vector;
	}

	private static Scalar createScalar(DATA_TYPE dataType, LocalDate val) {
		switch (dataType) {
			case DT_DATE:
				return new BasicDate(val);
			case DT_MONTH:
				return new BasicMonth(val.getYear(),val.getMonth());
			default:
				throw new RuntimeException("Failed to insert data. Cannot convert LocalDate to " + dataType + ".");
		}
	}

	private static Scalar createScalar(DATA_TYPE dataType, LocalDateTime val) {
		switch (dataType) {
			case DT_DATETIME:
				return new BasicDateTime(val);
			case DT_DATEHOUR:
				return new BasicDateHour(val);
			case DT_TIMESTAMP:
				return new BasicTimestamp(val);
			case DT_NANOTIME:
				return new BasicNanoTime(val);
			case DT_NANOTIMESTAMP:
				return new BasicNanoTimestamp(val);
			default:
				throw new RuntimeException("Failed to insert data. Cannot convert LocalDateTime to " + dataType + ".");
		}
	}

	private static Scalar createScalar(DATA_TYPE dataType, LocalTime val) {
		switch (dataType) {
			case DT_TIME:
				return new BasicTime(val);
			case DT_SECOND:
				return new BasicSecond(val);
			case DT_MINUTE:
				return new BasicMinute(val);
			case DT_NANOTIME:
				return new BasicNanoTime(val);
			default:
				throw new RuntimeException("Failed to insert data. Cannot convert LocalTime to " + dataType + ".");
		}
	}

	private static Scalar createScalar(DATA_TYPE dataType, Date val) {
		Calendar calendar=Calendar.getInstance();
		calendar.setTime(val);
		return createScalar(dataType, calendar);
	}

	private static Scalar createScalar(DATA_TYPE dataType, Calendar val) {
		switch (dataType) {
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
			case DT_TIMESTAMP:
				return new BasicTimestamp(val);
			default:
				throw new RuntimeException("Failed to insert data. Cannot convert Calendar to " + dataType + ".");
		}
	}

	private static Scalar createScalar(DATA_TYPE dataType, Entity val) {
		if ((val.isScalar() && val.getDataType() == dataType) || (Utils.getCategory(dataType) == Entity.DATA_CATEGORY.LITERAL && Utils.getCategory(val.getDataType()) == Entity.DATA_CATEGORY.LITERAL))
			return (Scalar) val;
		else
			throw new RuntimeException("Failed to insert data. Cannot convert Entity to " + dataType + ".");
	}

	private static Scalar createScalar(DATA_TYPE dataType, boolean val) {
		switch (dataType) {
			case DT_BOOL:
				return new BasicBoolean(val);
			default:
				throw new RuntimeException("Failed to insert data. Cannot convert boolean to " + dataType + ".");
		}
	}

	private static Scalar createScalar(DATA_TYPE dataType, char val) {
		if (val >= Byte.MIN_VALUE && val <= Byte.MAX_VALUE)
			return createScalar(dataType,(byte)val);
		else
			throw new RuntimeException("Failed to insert data, char cannot be converted because it exceeds the range of " + dataType + ".");
	}

	private static Scalar createScalar(DATA_TYPE dataType, byte val) {
		switch (dataType) {
			case DT_BYTE:
				return new BasicByte(val);
			case DT_SHORT:
				return new BasicShort(val);
			case DT_INT:
				return new BasicInt(val);
			case DT_LONG:
				return new BasicLong(val);
			case DT_FLOAT:
				return new BasicFloat(val);
			case DT_DOUBLE:
				return new BasicDouble(val);
			default:
				throw new RuntimeException("Failed to insert data. Cannot convert byte to " + dataType + ".");
		}
	}

	private static Scalar createScalar(DATA_TYPE dataType, short val) {
		switch (dataType) {
			case DT_BYTE:
				if(val >= Byte.MIN_VALUE && val <= Byte.MAX_VALUE)
					return new BasicByte((byte)val);
				else
					throw new RuntimeException("Failed to insert data, short cannot be converted because it exceeds the range of " + dataType + ".");
			case DT_SHORT:
				return new BasicShort(val);
			case DT_INT:
				return new BasicInt(val);
			case DT_LONG:
				return new BasicLong(val);
			case DT_FLOAT:
				return new BasicFloat(val);
			case DT_DOUBLE:
				return new BasicDouble(val);
			default:
				throw new RuntimeException("Failed to insert data. Cannot convert short to " + dataType + ".");
		}
	}

	private static Scalar createScalar(DATA_TYPE dataType, String val, int extraParam) {
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
				return new BasicString(val, dataType==DATA_TYPE.DT_BLOB);
			}
			case DT_STRING:
				return new BasicString(val);
			case DT_DECIMAL32:
				return new BasicDecimal32(val, extraParam);
			case DT_DECIMAL64:
				return new BasicDecimal64(val, extraParam);
			case DT_DECIMAL128:
				return new BasicDecimal128(val, extraParam);
			default:
				throw new RuntimeException("Failed to insert data. Cannot convert String to " + dataType + ".");
		}
	}

	private static Scalar createScalar(DATA_TYPE dataType, float val, int extraParam) {
		switch (dataType) {
			case DT_FLOAT:
				return new BasicFloat(val);
			case DT_DOUBLE:
				return new BasicDouble(val);
			case DT_DECIMAL32:
				return new BasicDecimal32(Float.toString(val), extraParam);
			case DT_DECIMAL64:
				return new BasicDecimal64(Float.toString(val), extraParam);
			default:
				throw new RuntimeException("Failed to insert data. Cannot convert float to " + dataType + ".");
		}
	}

	private static Scalar createScalar(DATA_TYPE dataType, double val, int extraParam) {
		switch (dataType) {
			case DT_FLOAT:
				if(val >= Float.MIN_VALUE && val <= Float.MAX_VALUE || val >= (-Float.MAX_VALUE) && val <= (-Float.MIN_VALUE))
					return new BasicFloat((float)val);
				else{
					throw new RuntimeException("Failed to insert data, double cannot be converted because it exceeds the range of " + dataType + ".");
				}
			case DT_DOUBLE:
				return new BasicDouble(val);
			case DT_DECIMAL32:
				return new BasicDecimal32(Double.toString(val), extraParam);
			case DT_DECIMAL64:
				return new BasicDecimal64(Double.toString(val), extraParam);
			default:
				throw new RuntimeException("Failed to insert data. Cannot convert double to " + dataType + ".");
		}
	}

	private static Scalar createScalar(DATA_TYPE dataType, int val, int extraParam) {
		switch (dataType) {
			case DT_BYTE:
				if(val >= Byte.MIN_VALUE && val <= Byte.MAX_VALUE)
					return new BasicByte((byte)val);
				else
					throw new RuntimeException("Failed to insert data, int cannot be converted because it exceeds the range of " + dataType + ".");
			case DT_SHORT:
				if(val >= Short.MIN_VALUE && val <= Short.MAX_VALUE)
					return new BasicShort((short)val);
				else
					throw new RuntimeException("Failed to insert data, int cannot be converted because it exceeds the range of " + dataType + ".");
			case DT_INT:
				return new BasicInt(val);
			case DT_LONG:
				return new BasicLong(val);
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
			case DT_DECIMAL32:
				return new BasicDecimal32(val, extraParam);
			case DT_DECIMAL64:
				return new BasicDecimal64((long) val, extraParam);
			case DT_DECIMAL128:
				return new BasicDecimal128(BigInteger.valueOf(val), extraParam);
			case DT_FLOAT:
				return new BasicFloat(val);
			case DT_DOUBLE:
				return new BasicDouble(val);
			default:
				throw new RuntimeException("Failed to insert data. Cannot convert int to " + dataType + ".");
		}
	}

	private static Scalar createScalar(DATA_TYPE dataType, long val, int extraParam){
		switch (dataType) {
			case DT_BYTE:
				if(val >= Byte.MIN_VALUE && val <= Byte.MAX_VALUE)
					return new BasicByte((byte)val);
				else
					throw new RuntimeException("Failed to insert data, long cannot be converted because it exceeds the range of " + dataType + ".");
			case DT_SHORT:
				if(val >= Short.MIN_VALUE && val <= Short.MAX_VALUE)
					return new BasicShort((short)val);
				else
					throw new RuntimeException("Failed to insert data, long cannot be converted because it exceeds the range of " + dataType + ".");
			case DT_INT:
				if(val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE)
					return new BasicInt((int)val);
				else
					throw new RuntimeException("Failed to insert data, long cannot be converted because it exceeds the range of " + dataType + ".");
			case DT_LONG:
				return new BasicLong(val);
			case DT_NANOTIME:
				return new BasicNanoTime(val);
			case DT_NANOTIMESTAMP:
				return new BasicNanoTimestamp(val);
			case DT_TIMESTAMP:
				return new BasicTimestamp(val);
			case DT_DECIMAL32:
				if(val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE)
					return new BasicDecimal32(Long.toString(val), extraParam);
				else
					throw new RuntimeException("Failed to insert data, long cannot be converted because it exceeds the range of " + dataType + ".");
			case DT_DECIMAL64:
				return new BasicDecimal64(val, extraParam);
			case DT_FLOAT:
				return new BasicFloat(val);
			case DT_DOUBLE:
				return new BasicDouble(val);
			default:
				throw new RuntimeException("Failed to insert data. Cannot convert long to " + dataType + ".");
		}
	}
}
