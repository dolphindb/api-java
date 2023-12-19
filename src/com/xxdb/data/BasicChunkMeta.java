package com.xxdb.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.xxdb.io.AbstractExtendedDataOutputStream;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

public class BasicChunkMeta extends AbstractEntity implements Entity{
	private String path;
	private BasicUuid id;
	private int version;
	private int size;
	private byte flag;
	private List<String> sites;

	public BasicChunkMeta(ExtendedDataInput in) throws IOException{
		in.readShort(); //skip the length of the data
		path = in.readString();
		id = new BasicUuid(in);
		version = in.readInt();
		size = in.readInt();
		flag = in.readByte();
		sites = new ArrayList<>();
		int copyCount = in.readByte();
		for(int i=0; i<copyCount; ++i)
			sites.add(in.readString());
	}
	
	public String getPath(){
		return path;
	}
	
	public BasicUuid getId(){
		return id;
	}
	
	public int getVersion(){
		return version;
	}
	
	public int size(){
		return size;
	}
	
	public int getCopyCount(){
		return sites.size();
	}
	
	public boolean isFileBlock(){
		return (flag & 3) == 1;
	}
	
	public boolean isTablet(){
		return (flag & 3) == 0;
	}
	
	public boolean isSplittable(){
		return (flag & 4) == 1;
	}
	
	@Override
	public DATA_FORM getDataForm() {
		return DATA_FORM.DF_CHUNK;
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return null;
	}

	@Override
	public DATA_TYPE getDataType() {
		return null;
	}

	@Override
	public int rows() {
		return 1;
	}

	@Override
	public int columns() {
		return 0;
	}

	@Override
	public String getString() {
		StringBuilder str = new StringBuilder(isTablet() ? "Tablet[" : "FileBlock[");
		str.append(path);
		str.append(", ");
		str.append(id.getString());
		str.append(", {");
		for(int i=0; i<sites.size(); ++i){
			if(i>0)
				str.append(", ");
			str.append(sites.get(i));
		}
		str.append("}, v");
		str.append(version);
		str.append(", ");
		str.append(size);
		if(isSplittable())
			str.append(", splittable]");
		else
			str.append("]");
		return str.toString();
	}

	@Override
	public void write(ExtendedDataOutput output) throws IOException {
		int length = 27 + AbstractExtendedDataOutputStream.getUTFlength(path, 0, 0) + sites.size();
		for(String site : sites)
			length += AbstractExtendedDataOutputStream.getUTFlength(site, 0, 0);
		output.writeShort(length);
		output.writeString(path);
		output.writeLong2(id.getLong2());
		output.writeInt(version);
		output.writeInt(size);
		output.writeByte(flag);
		output.writeByte(sites.size());
		for(String site : sites)
			output.writeUTF(site);
	}
}
