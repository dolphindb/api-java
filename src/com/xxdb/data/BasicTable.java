package com.xxdb.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.xxdb.compression.VectorDecompressor;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

/**
 * 
 * Corresponds to DolphinDB table object
 *
 */

public class BasicTable extends AbstractEntity implements Table{
	private List<Vector> columns_ = new ArrayList<Vector>();
	private List<String> names_ = new ArrayList<String>();
	private Map<String, Integer> name2index_ = new HashMap<String, Integer>();

	public BasicTable(ExtendedDataInput in) throws IOException{
		int rows = in.readInt();
		int cols = in.readInt();
		String tableName = in.readString(); 
		
		//read column names
		for(int i=0; i<cols; ++i){
			String name = in.readString();
			name2index_.put(name, name2index_.size());
			names_.add(name);
		}
		
		VectorDecompressor decompressor = null;
		SymbolBaseCollection collection = null;
		//read columns
		for(int i=0; i<cols; ++i){
			short flag = in.readShort();
			int form = flag>>8;
			int type = flag & 0xff;
            boolean extended = type >= 128;
            if(type >= 128)
            	type -= 128;

			DATA_FORM df = DATA_FORM.values()[form];
			DATA_TYPE dt = DATA_TYPE.valueOf(type);
			if(df != DATA_FORM.DF_VECTOR)
				throw new IOException("Invalid form for column [" + names_.get(i) + "] for table " + tableName);
			Vector vector;
			if(dt == DATA_TYPE.DT_SYMBOL && extended){
				if(collection == null)
					collection = new SymbolBaseCollection();
				vector = new BasicSymbolVector(df, in, collection);
			} else if (dt == DATA_TYPE.DT_COMPRESS) {
				if(decompressor == null)
					decompressor = new VectorDecompressor();
				vector = decompressor.decompress(BasicEntityFactory.instance(), in, extended, true);
			}
			else{
				vector = (Vector)BasicEntityFactory.instance().createEntity(df, dt, in, extended);
			}
			if(vector.rows() != rows && vector.rows()!= 1)
				throw new IOException("The number of rows for column " +names_.get(i) + " is not consistent with other columns");
			columns_.add(vector);
		}
		if(collection != null)
			collection.clear();
	}
	
    public BasicTable(final List<String> colNames, final List<Vector> cols) {
        this.setColName(colNames);
        this.setColumns(cols);
    }
    
    public void setColName (final List<String> colNames) {
        names_.clear();
		name2index_.clear();
        for (String name : colNames){
			names_.add(name);
			name2index_.put(name, name2index_.size());
		}

    }
    
    public void setColumns (final List<Vector> cols) {
    	columns_.clear();
        // this is a shallow copy!
        for (Vector vector : cols)
        	columns_.add(vector);
        
    }
    
	@Override
	public DATA_CATEGORY getDataCategory() {
		return DATA_CATEGORY.MIXED;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_DICTIONARY;
	}
	
	@Override
	public DATA_FORM getDataForm() {
		return DATA_FORM.DF_TABLE;
	}

	@Override
	public int rows() {
		if(columns()<=0)
			return 0;
		else
			return columns_.get(0).rows();
	}

	@Override
	public int columns() {
		return columns_.size();
	}

	@Override
	public Vector getColumn(int index) {
		return columns_.get(index);
	}

	@Override
	public Vector getColumn(String name) {
		Integer index = name2index_.get(name);
		if(index == null)
			return null;
		else
			return getColumn(index);
	}
	
	public String getColumnName(int index){
		return names_.get(index);
	}

	public String getString(){
		int rows = Math.min(Utils.DISPLAY_ROWS,rows());
	    int strColMaxWidth = Utils.DISPLAY_WIDTH/Math.min(columns(),Utils.DISPLAY_COLS)+5;
	    int length=0;
	    int curCol=0;
	    int maxColWidth;
		StringBuilder[] list = new StringBuilder[rows+1];
		StringBuilder separator = new StringBuilder();
		String[] listTmp = new String[rows+1];
	    int i,curSize;

	    for(i=0; i<list.length; ++i)
	    	list[i] = new StringBuilder();
	    
	    while(length<Utils.DISPLAY_WIDTH && curCol<columns()){
	    	listTmp[0]=getColumnName(curCol);
	    	maxColWidth=0;
	    	for(i=0;i<rows;i++){
	    		listTmp[i+1]=getColumn(curCol).get(i).getString();
	    		if(listTmp[i+1].length()>maxColWidth)
	    			maxColWidth=listTmp[i+1].length();
	    	}
	    	if(maxColWidth>strColMaxWidth && getColumn(curCol).getDataCategory()==DATA_CATEGORY.LITERAL)
	    		maxColWidth=strColMaxWidth;
	    	if((int)listTmp[0].length()>maxColWidth)
	    		maxColWidth=Math.min(strColMaxWidth,(int)listTmp[0].length());

	    	if(length+maxColWidth>Utils.DISPLAY_WIDTH && curCol+1<columns())
	    		break;

	    	for(int k=0; k<maxColWidth; ++k)
	    		separator.append('-');
	    	if(curCol<columns()-1){
	    		maxColWidth++;
	    		separator.append(' ');
	    	}
	    	
	    	for(i=0;i<=rows;i++){
	    		curSize=listTmp[i].length();
	    		if(curSize<=maxColWidth){
	    			list[i].append(listTmp[i]);
	    			if(curSize<maxColWidth){
	    				for(int j=0; j<(maxColWidth-curSize); j++)
	    					list[i].append(' ');
	    			}
	    		}
	    		else{
	    			if(maxColWidth>3)
	    				list[i].append(listTmp[i].substring(0,maxColWidth-3));
	    			list[i].append("...");
	    			separator.append("---");
	    		}
	    	}
	    	length+=maxColWidth;
	    	curCol++;
	    }

	    if(curCol<columns()){
	    	for(i=0;i<=rows;i++)
	    		list[i].append("...");
	    }

	    StringBuilder resultStr = new StringBuilder(list[0]);
	    resultStr.append("\n");
	    resultStr.append(separator);
	    resultStr.append("\n");
	    for(i=1;i<=rows;i++){
	    	resultStr.append(list[i]);
	    	resultStr.append("\n");
	    }
	    if(rows<rows())
	    	resultStr.append("...\n");
	    return resultStr.toString();
	}

	public String getRowJson(int rowIndex){
		StringBuilder jsonStr = new StringBuilder();
		if(rowIndex<rows()){
			jsonStr.append("{");
			for(int i=0;i<names_.size();i++){
				jsonStr.append(names_.get(i));
				jsonStr.append(":");
				jsonStr.append(columns_.get(i).get(rowIndex).getJsonString());
				if(i<names_.size()-1)
					jsonStr.append(",");
			}
			jsonStr.delete(jsonStr.length()-1,jsonStr.length()-1);
			jsonStr.append("}");
		}
		return jsonStr.toString();
	}

	public void write(ExtendedDataOutput out) throws IOException{
		int flag = (DATA_FORM.DF_TABLE.ordinal() << 8) + getDataType().getValue();
		out.writeShort(flag);
		out.writeInt(rows());
		out.writeInt(columns());
		out.writeString(""); //table name
		for(String colName : names_)
			out.writeString(colName);
		SymbolBaseCollection collection = null;
		for(Vector vector : columns_){
			if(vector instanceof BasicSymbolVector){
				if(collection == null)
					collection = new SymbolBaseCollection();
				((BasicSymbolVector)vector).write(out, collection);
			}
			else
				vector.write(out);
		}
	}

	@Override
	public void writeCompressed(ExtendedDataOutput output) throws IOException {
		short flag = (short) (Entity.DATA_FORM.DF_TABLE.ordinal() << 8 | 8 & 0xff); //8: table type TODO: add table type
		output.writeShort(flag);

		int rows = this.rows();
		int cols = this.columns();
		output.writeInt(rows);
		output.writeInt(cols);
		output.writeString(""); //table name
		for (int i = 0; i < cols; i++) {
			output.writeString(this.getColumnName(i));
		}

		for (int i = 0; i < cols; i++) {
			AbstractVector v = (AbstractVector) this.getColumn(i);
			if(v.getDataType() == DATA_TYPE.DT_SYMBOL)
				v.write(output);
			else
				v.writeCompressed(output);
		}

	}

	public BasicTable combine(BasicTable table){
		List<Vector> newCol = new ArrayList<>();
		for (int i=0; i< this.columns();i++) {
			newCol.add(this.getColumn(i).combine(table.getColumn(i)));
		}
		return new BasicTable(this.names_,newCol);
	}
	
	public Table getSubTable(int[] indices){
		int colCount = columns_.size();
		List<Vector> cols = new ArrayList<Vector>(colCount);
		for(int i=0; i<colCount; ++i)
			cols.add(columns_.get(i).getSubVector(indices));
		return new BasicTable(names_, cols);
	}

	@Override
	public void addColumn(String colName, Vector col) {
		names_.add(colName);
		name2index_.put(colName, name2index_.size());
		columns_.add(col);
	}
}
