package com.xxdb.data;

public abstract class AbstractTensor  extends AbstractEntity implements Tensor {

    @Override
    public DATA_FORM getDataForm() {
        return DATA_FORM.DF_TENSOR;
    }

    @Override
    public int columns() {
        return 1;
    }

}
