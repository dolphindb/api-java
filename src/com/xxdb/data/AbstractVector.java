package com.xxdb.data;

import java.io.IOException;

import com.xxdb.io.ExtendedDataOutput;

public abstract class AbstractVector extends AbstractEntity implements Vector{
	private DATA_FORM df_;
	
	protected abstract void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException;
	
	public AbstractVector(DATA_FORM df){
		df_ = df;
	}
	
	@Override
	public DATA_FORM getDataForm() {
		return df_;
	}
	
	@Override
	public int columns() {
		return 1;
	}
	
	public String getString(){
		StringBuilder sb = new StringBuilder("[");
		int size = Math.min(DISPLAY_ROWS, rows());
		if(size > 0)
			sb.append(get(0).getString());
		for(int i=1; i<size; ++i){
			sb.append(',');
			sb.append(get(i).getString());
		}
		if(size < rows())
			sb.append(",...");
		sb.append("]");
		return sb.toString();
	}
	
	public void write(ExtendedDataOutput out) throws IOException{
		int flag = (df_.ordinal() << 8) + getDataType().ordinal();
		out.writeShort(flag);
		out.writeInt(rows());
		out.writeInt(columns());
		writeVectorToOutputStream(out);
	}
}
