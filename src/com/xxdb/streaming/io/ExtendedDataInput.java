package com.xxdb.streaming.io;

import java.io.DataInput;
import java.io.IOException;

public interface ExtendedDataInput extends DataInput{
	String readString() throws IOException;
}
