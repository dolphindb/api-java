package com.xxdb.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
		
		BasicEntityFactory factory = new BasicEntityFactory();
		//read columns
		for(int i=0; i<cols; ++i){
			short flag = in.readShort();
			int form = flag>>8;
			int type = flag & 0xff;
			
			DATA_FORM df = DATA_FORM.values()[form];
			DATA_TYPE dt = DATA_TYPE.values()[type];
			if(df != DATA_FORM.DF_VECTOR)
				throw new IOException("Invalid form for column [" + names_.get(i) + "] for table " + tableName);
			Vector vector = (Vector)factory.createEntity(df, dt, in);
			if(vector.rows() != rows && vector.rows()!= 1)
				throw new IOException("The number of rows for column " +names_.get(i) + " is not consistent with other columns");
			columns_.add(vector);
		}
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
	
	public void write(ExtendedDataOutput out) throws IOException{
		int flag = (DATA_FORM.DF_TABLE.ordinal() << 8) + getDataType().ordinal();
		out.writeShort(flag);
		out.writeInt(rows());
		out.writeInt(columns());
		out.writeString(""); //table name
		for(String colName : names_)
			out.writeString(colName);
		for(Vector vector : columns_)
			vector.write(out);
	}
}
