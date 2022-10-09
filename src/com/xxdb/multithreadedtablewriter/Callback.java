package com.xxdb.multithreadedtablewriter;

import com.xxdb.data.Table;

public interface Callback {
    void writeCompletion(Table callbackTable);
}
