package com.xxdb.data;

import java.io.IOException;

import com.xxdb.io.ExtendedDataInput;

public interface EntityFactory {
	Entity createEntity(Entity.DATA_FORM form, Entity.DATA_TYPE type, ExtendedDataInput in, boolean exteneded)  throws IOException;
	Matrix createMatrixWithDefaultValue(Entity.DATA_TYPE type, int rows, int columns);
	Vector createVectorWithDefaultValue(Entity.DATA_TYPE type, int size);
	Vector createPairWithDefaultValue(Entity.DATA_TYPE type);
	Scalar createScalarWithDefaultValue(Entity.DATA_TYPE type);
}
