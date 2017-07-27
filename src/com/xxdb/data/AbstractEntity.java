package com.xxdb.data;

public abstract class AbstractEntity {
	public abstract Entity.DATA_FORM getDataForm();
	
	public boolean isScalar(){ return getDataForm() == Entity.DATA_FORM.DF_SCALAR;}
	public boolean isVector(){ return getDataForm() == Entity.DATA_FORM.DF_VECTOR;}
	public boolean isPair(){ return getDataForm() == Entity.DATA_FORM.DF_PAIR;}
	public boolean isTable(){ return getDataForm() == Entity.DATA_FORM.DF_TABLE;}
	public boolean isMatrix(){ return getDataForm() == Entity.DATA_FORM.DF_MATRIX;}
	public boolean isDictionary(){ return getDataForm() == Entity.DATA_FORM.DF_DICTIONARY;}
	public boolean isChart(){ return getDataForm() == Entity.DATA_FORM.DF_CHART;}
	
	protected Entity.DATA_CATEGORY getDataCategory(Entity.DATA_TYPE valueType) {
		if(valueType == Entity.DATA_TYPE.DT_BOOL)
			return Entity.DATA_CATEGORY.LOGICAL;
		else if(valueType == Entity.DATA_TYPE.DT_STRING || valueType == Entity.DATA_TYPE.DT_SYMBOL)
			return Entity.DATA_CATEGORY.LITERAL;
		else if(valueType == Entity.DATA_TYPE.DT_DOUBLE || valueType == Entity.DATA_TYPE.DT_FLOAT)
			return Entity.DATA_CATEGORY.FLOATING;
		else if(valueType == Entity.DATA_TYPE.DT_BYTE || valueType == Entity.DATA_TYPE.DT_SHORT || valueType == Entity.DATA_TYPE.DT_INT || valueType == Entity.DATA_TYPE.DT_LONG)
			return Entity.DATA_CATEGORY.INTEGRAL;
		else if(valueType == Entity.DATA_TYPE.DT_FUNCTIONDEF || valueType == Entity.DATA_TYPE.DT_HANDLE)
			return Entity.DATA_CATEGORY.SYSTEM;
		else if(valueType == Entity.DATA_TYPE.DT_VOID)
			return Entity.DATA_CATEGORY.NOTHING;
		else if(valueType == Entity.DATA_TYPE.DT_ANY)
			return Entity.DATA_CATEGORY.MIXED;
		else if(valueType == Entity.DATA_TYPE.DT_TIMESTAMP)
			return Entity.DATA_CATEGORY.TEMPORAL;
		else 
			return Entity.DATA_CATEGORY.TEMPORAL;
	}
}
