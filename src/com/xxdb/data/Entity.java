package com.xxdb.data;

import java.io.IOException;

import com.xxdb.io.ExtendedDataOutput;


public interface Entity {
	enum DATA_TYPE {DT_VOID,DT_BOOL,DT_BYTE,DT_SHORT,DT_INT,DT_LONG,DT_DATE,DT_MONTH,DT_TIME,DT_MINUTE,DT_SECOND,DT_DATETIME,DT_TIMESTAMP,DT_NANOTIME,DT_NANOTIMESTAMP,
		DT_FLOAT,DT_DOUBLE,DT_SYMBOL,DT_STRING,DT_FUNCTIONDEF,DT_HANDLE,DT_CODE,DT_DATASOURCE,DT_RESOURCE,DT_ANY,DT_DICTIONARY,DT_OBJECT};
	enum DATA_CATEGORY {NOTHING,LOGICAL,INTEGRAL,FLOATING,TEMPORAL,LITERAL,SYSTEM,MIXED};
	enum DATA_FORM {DF_SCALAR,DF_VECTOR,DF_PAIR,DF_MATRIX,DF_SET,DF_DICTIONARY,DF_TABLE,DF_CHART,DF_CHUNK};
	enum PARTITION_TYPE {SEQ, VALUE, RANGE, LIST}
	DATA_FORM getDataForm();
	DATA_CATEGORY getDataCategory();
	DATA_TYPE getDataType();
	int rows();
	int columns();
	String getString();
	void write(ExtendedDataOutput output) throws IOException;

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
				|| type == Entity.DATA_TYPE.DT_DATETIME || type == Entity.DATA_TYPE.DT_MONTH || type == Entity.DATA_TYPE.DT_TIMESTAMP || type == DATA_TYPE.DT_NANOTIME || type == DATA_TYPE.DT_NANOTIMESTAMP)
			return Entity.DATA_CATEGORY.TEMPORAL;
		else if(type == Entity.DATA_TYPE.DT_INT || type == Entity.DATA_TYPE.DT_LONG || type == Entity.DATA_TYPE.DT_SHORT || type == Entity.DATA_TYPE.DT_BYTE)
			return Entity.DATA_CATEGORY.INTEGRAL;
		else if(type == Entity.DATA_TYPE.DT_BOOL)
			return Entity.DATA_CATEGORY.LOGICAL;
		else if(type == Entity.DATA_TYPE.DT_DOUBLE || type == Entity.DATA_TYPE.DT_FLOAT)
			return Entity.DATA_CATEGORY.FLOATING;
		else if(type == Entity.DATA_TYPE.DT_STRING || type == Entity.DATA_TYPE.DT_SYMBOL)
			return Entity.DATA_CATEGORY.LITERAL;
		else if(type == Entity.DATA_TYPE.DT_ANY)
			return Entity.DATA_CATEGORY.MIXED;
		else if(type == Entity.DATA_TYPE.DT_VOID)
			return Entity.DATA_CATEGORY.NOTHING;
		else
			return Entity.DATA_CATEGORY.SYSTEM;
	}
}
