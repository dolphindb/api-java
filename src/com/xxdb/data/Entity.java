package com.xxdb.data;

import java.io.IOException;

import com.xxdb.io.ExtendedDataOutput;

public interface Entity {
	enum DATA_TYPE {DT_VOID,DT_BOOL,DT_BYTE,DT_SHORT,DT_INT,DT_LONG,DT_DATE,DT_MONTH,DT_TIME,DT_MINUTE,DT_SECOND,DT_DATETIME,
		DT_TIMESTAMP,DT_FLOAT,DT_DOUBLE,DT_SYMBOL,DT_STRING,DT_FUNCTIONDEF,DT_HANDLE,DT_CODE,DT_DATASOURCE,DT_ANY,DT_DICTIONARY,DT_RESOURCE,DT_OBJECT};
	enum DATA_CATEGORY {NOTHING,LOGICAL,INTEGRAL,FLOATING,TEMPORAL,LITERAL,SYSTEM,MIXED};
	enum DATA_FORM {DF_SCALAR,DF_VECTOR,DF_PAIR,DF_MATRIX,DF_SET,DF_DICTIONARY,DF_TABLE,DF_CHART};
	
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
}
