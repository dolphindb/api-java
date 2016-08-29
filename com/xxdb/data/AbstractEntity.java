package com.xxdb.data;

import com.xxdb.data.Entity.DATA_CATEGORY;
import com.xxdb.data.Entity.DATA_FORM;
import com.xxdb.data.Entity.DATA_TYPE;

public abstract class AbstractEntity {
	public abstract DATA_FORM getDataForm();
	
	public boolean isScalar(){ return getDataForm() == Entity.DATA_FORM.DF_SCALAR;}
	public boolean isVector(){ return getDataForm() == Entity.DATA_FORM.DF_VECTOR;}
	public boolean isPair(){ return getDataForm() == Entity.DATA_FORM.DF_PAIR;}
	public boolean isTable(){ return getDataForm() == Entity.DATA_FORM.DF_TABLE;}
	public boolean isMatrix(){ return getDataForm() == Entity.DATA_FORM.DF_MATRIX;}
	public boolean isDictionary(){ return getDataForm() == Entity.DATA_FORM.DF_DICTIONARY;}
	public boolean isChart(){ return getDataForm() == Entity.DATA_FORM.DF_CHART;}
	
	protected DATA_CATEGORY getDataCategory(DATA_TYPE valueType) {
		if(valueType == DATA_TYPE.DT_BOOL)
			return DATA_CATEGORY.LOGICAL;
		else if(valueType == DATA_TYPE.DT_STRING || valueType == DATA_TYPE.DT_SYMBOL)
			return DATA_CATEGORY.LITERAL;
		else if(valueType == DATA_TYPE.DT_DOUBLE || valueType == DATA_TYPE.DT_FLOAT)
			return DATA_CATEGORY.FLOATING;
		else if(valueType == DATA_TYPE.DT_BYTE || valueType == DATA_TYPE.DT_SHORT || valueType == DATA_TYPE.DT_INT || valueType == DATA_TYPE.DT_LONG)
			return DATA_CATEGORY.INTEGRAL;
		else if(valueType == DATA_TYPE.DT_FUNCTIONDEF || valueType == DATA_TYPE.DT_HANDLE)
			return DATA_CATEGORY.SYSTEM;
		else if(valueType == DATA_TYPE.DT_VOID)
			return DATA_CATEGORY.NOTHING;
		else if(valueType == DATA_TYPE.DT_ANY)
			return DATA_CATEGORY.MIXED;
		else if(valueType == DATA_TYPE.DT_TIMESTAMP)
			return DATA_CATEGORY.TEMPORAL;
		else 
			return DATA_CATEGORY.TEMPORAL;
	}
}
