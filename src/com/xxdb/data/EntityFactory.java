package com.xxdb.data;

import java.io.IOException;

import com.xxdb.data.Entity.DATA_FORM;
import com.xxdb.data.Entity.DATA_TYPE;
import com.xxdb.io.ExtendedDataInput;

public interface EntityFactory {
	Entity createEntity(DATA_FORM form, DATA_TYPE type, ExtendedDataInput in)  throws IOException;
	Matrix createMatrixWithDefaultValue(DATA_TYPE type, int rows, int columns);
	Vector createVectorWithDefaultValue(DATA_TYPE type, int size);
	Vector createPairWithDefaultValue(DATA_TYPE type);
	Scalar createScalarWithDefaultValue(DATA_TYPE type);
}
