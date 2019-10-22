package com.xxdb.io;

import java.io.DataInput;
import java.io.IOException;

public interface ExtendedDataInput extends DataInput{
	boolean isLittleEndian();
	String readString() throws IOException;
	Long2 readLong2() throws IOException;
}
