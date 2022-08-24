package com.xxdb.data;

import java.io.IOException;
import java.util.HashMap;

import com.xxdb.io.ExtendedDataOutput;


public interface Entity {
	public enum DATA_TYPE {DT_VOID("VOID", 0), DT_BOOL("BOOL", 1), DT_BYTE("CHAR", 2), DT_SHORT("SHORT", 3), DT_INT("INT", 4), DT_LONG("LONG", 5),
					DT_DATE("DATE", 6), DT_MONTH("MONTH", 7), DT_TIME("TIME", 8), DT_MINUTE("MINUTE", 9), DT_SECOND("SECOND", 10), 
					DT_DATETIME("DATETIME", 11), DT_TIMESTAMP("TIMESTAMP", 12), DT_NANOTIME("NANOTIME", 13), DT_NANOTIMESTAMP("NANOTIMESTAMP", 14),	DT_FLOAT("FLOAT", 15),
					DT_DOUBLE("DOUBLE", 16), DT_SYMBOL("SYMBOL", 17), DT_STRING("STRING", 18), DT_UUID("UUID", 19), DT_FUNCTIONDEF("FUNCTIONDEF", 20),
					DT_HANDLE("HANDLE", 21), DT_CODE("CODE", 22), DT_DATASOURCE("DATASOURCE", 23), DT_RESOURCE("RESOURCE", 24), DT_ANY("ANY", 25),
					DT_COMPRESS("COMPRESSED", 26), DT_DICTIONARY("DICTIONARY", 27), DT_DATEHOUR("DATEHOUR", 28), DT_DATEMINUTE("DATEMINUTE", 29),	DT_IPADDR("IPADDR", 30),
					DT_INT128("INT128", 31), DT_BLOB("BLOB", 32), DT_DECIMAL("DECIMAL", 33), DT_COMPLEX("COMPLEX", 34), DT_POINT("POINT", 35), 
					DT_DURATION("DURATION", 36), DT_DECIMAL32("DECIMAL32", 37), DT_DECIMAL64("DECIMAL64", 38), DT_DECIMAL128("DECIMAL128", 39),DT_OBJECT("OBJECT", 40),
					
					DT_BOOL_ARRAY("BOOL[]", 65), DT_BYTE_ARRAY("CHAR[]", 66), DT_SHORT_ARRAY("SHORT[]", 67), DT_INT_ARRAY("INT[]", 68), DT_LONG_ARRAY("LONG[]", 69),
					DT_DATE_ARRAY("DATE[]", 70), DT_MONTH_ARRAY("MONTH[]", 71), DT_TIME_ARRAY("TIME[]", 72), DT_MINUTE_ARRAY("MINUTE[]", 73), DT_SECOND_ARRAY("SECOND[]", 74),
					DT_DATETIME_ARRAY("DATETIME[]", 75), DT_TIMESTAMP_ARRAY("TIMESTAMP[]", 76), DT_NANOTIME_ARRAY("NANOTIME[]", 77), DT_NANOTIMESTAMP_ARRAY("NANOTIMESTAMP[]", 78), DT_FLOAT_ARRAY("FLOAT[]", 79),
					DT_DOUBLE_ARRAY("DOUBLE[]", 80), DT_SYMBOL_ARRAY("SYMBOL[]", 81), DT_STRING_ARRAY("STRING[]", 82), DT_UUID_ARRAY("UUID[]", 83), DT_DATEHOUR_ARRAY("DATEHOUR[]", 92),
					DT_DATEMINUTE_ARRAY("DATEMINUTE[]", 93), DT_IPADDR_ARRAY("IPADDR[]", 94), DT_INT128_ARRAY("INT128[]", 95), DT_DECIMAL_ARRAY("DECIMAL[]", 97), DT_COMPLEX_ARRAY("COMPLEX[]", 98),
					DT_POINT_ARRAY("POINT[]", 99);
		
		DATA_TYPE(String name, int value){
			this.name = name;
			this.value = value;
		}
		
		public String getName(){
			return name;
		}
		
		public int getValue(){
			return value;
		}
		
		public static DATA_TYPE valueOf(int type){
			DATA_TYPE enumType  = valueMap.get(type);
			if(enumType == null)
				throw new RuntimeException("Can't find enum DATA_TYPE for value " + String.valueOf(type));
			return enumType;
		}
		
		public static DATA_TYPE valueOfTypeName(String name){
			DATA_TYPE enumType  = nameMap.get(name);
			if(enumType == null)
				throw new RuntimeException("Can't find enum DATA_TYPE for type name " + name);
			return enumType;
		}
		
		private final String name;
		private final int value;
		private static HashMap<String, DATA_TYPE> nameMap = new HashMap<String, DATA_TYPE>();
		private static HashMap<Integer, DATA_TYPE> valueMap = new HashMap<Integer, DATA_TYPE>();
		
		static {
			for(DATA_TYPE type : DATA_TYPE.values()){
				nameMap.put(type.getName(), type);
				valueMap.put(type.getValue(), type);
			}
		}
	};
	
	enum DATA_CATEGORY {NOTHING,LOGICAL,INTEGRAL,FLOATING,TEMPORAL,LITERAL,SYSTEM,MIXED,BINARY,ARRAY,DECIMAL};
	enum DATA_FORM {DF_SCALAR,DF_VECTOR,DF_PAIR,DF_MATRIX,DF_SET,DF_DICTIONARY,DF_TABLE,DF_CHART,DF_CHUNK,DF_SYSOBJ};
	enum PARTITION_TYPE {SEQ, VALUE, RANGE, LIST, COMPO, HASH}
	enum DURATION {NS, US, MS, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR, BDAY};
	DATA_FORM getDataForm();
	DATA_CATEGORY getDataCategory();
	DATA_TYPE getDataType();
	int rows();
	int columns();
	String getString();
	void write(ExtendedDataOutput output) throws IOException;
	default void writeCompressed(ExtendedDataOutput output) throws IOException {
		throw new IOException("Only BasicTable and BasicVector support compression");
	}; //FIXME: "writeCompressed" modify name

	boolean isScalar();
	boolean isVector();
	boolean isPair();
	boolean isTable();
	boolean isMatrix();
	boolean isDictionary();
	boolean isChart();
	boolean isChunk();

	static Entity.DATA_CATEGORY typeToCategory(Entity.DATA_TYPE type) {
		if(type == Entity.DATA_TYPE.DT_TIME || type == Entity.DATA_TYPE.DT_SECOND || type== Entity.DATA_TYPE.DT_MINUTE || type == Entity.DATA_TYPE.DT_DATE
				|| type == Entity.DATA_TYPE.DT_DATETIME || type == Entity.DATA_TYPE.DT_MONTH || type == Entity.DATA_TYPE.DT_TIMESTAMP || type == DATA_TYPE.DT_NANOTIME || type == DATA_TYPE.DT_NANOTIMESTAMP
				|| type == Entity.DATA_TYPE.DT_DATEHOUR || type == Entity.DATA_TYPE.DT_DATEMINUTE)
			return Entity.DATA_CATEGORY.TEMPORAL;
		else if(type == Entity.DATA_TYPE.DT_INT || type == Entity.DATA_TYPE.DT_LONG || type == Entity.DATA_TYPE.DT_SHORT || type == Entity.DATA_TYPE.DT_BYTE)
			return Entity.DATA_CATEGORY.INTEGRAL;
		else if(type == Entity.DATA_TYPE.DT_BOOL)
			return Entity.DATA_CATEGORY.LOGICAL;
		else if(type == Entity.DATA_TYPE.DT_DOUBLE || type == Entity.DATA_TYPE.DT_FLOAT)
			return Entity.DATA_CATEGORY.FLOATING;
		else if(type == Entity.DATA_TYPE.DT_STRING || type == Entity.DATA_TYPE.DT_SYMBOL)
			return Entity.DATA_CATEGORY.LITERAL;
		else if(type==Entity.DATA_TYPE.DT_INT128 || type==Entity.DATA_TYPE.DT_UUID || type==Entity.DATA_TYPE.DT_IPADDR)
			return Entity.DATA_CATEGORY.BINARY;
		else if(type == Entity.DATA_TYPE.DT_ANY)
			return Entity.DATA_CATEGORY.MIXED;
		else if(type == Entity.DATA_TYPE.DT_VOID)
			return Entity.DATA_CATEGORY.NOTHING;
		else
			return Entity.DATA_CATEGORY.SYSTEM;
	}
}
