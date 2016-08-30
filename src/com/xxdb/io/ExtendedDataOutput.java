package com.xxdb.io;

import java.io.DataOutput;
import java.io.IOException;

public interface ExtendedDataOutput extends DataOutput {
	void writeString(String str) throws IOException;
	void flush() throws IOException;
}
