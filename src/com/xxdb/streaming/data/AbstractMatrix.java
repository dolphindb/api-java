package com.xxdb.streaming.data;

import java.io.IOException;

import com.xxdb.streaming.io.ExtendedDataInput;
import com.xxdb.streaming.io.ExtendedDataOutput;

public abstract class AbstractMatrix extends AbstractEntity implements Matrix{
	private Vector rowLabels = null;
	private Vector columnLabels = null;
	protected int rows;
	protected int columns;
	
	protected abstract void readMatrixFromInputStream(int rows, int columns, ExtendedDataInput in) throws IOException;
	protected abstract void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException;
	
	protected AbstractMatrix(int rows, int columns){
		this.rows = rows;
		this.columns = columns;
	}
	
	protected AbstractMatrix(ExtendedDataInput in) throws IOException{
		byte hasLabels = in.readByte();
		
		BasicEntityFactory factory = null;
		DATA_TYPE[] types = DATA_TYPE.values();
		if(hasLabels > 0)
			factory = new BasicEntityFactory();
		
		if((hasLabels & 1) == 1){
			//contain row labels
			short flag = in.readShort();
			int form = flag>>8;
			int type = flag & 0xff;
			if(form != DATA_FORM.DF_VECTOR.ordinal())
				throw new IOException("The form of matrix row labels must be vector");
			if(type <0 || type >= types.length)
				throw new IOException("Invalid data type for matrix row labels: " + type);
			rowLabels = (Vector)factory.createEntity(DATA_FORM.DF_VECTOR, types[type], in);
		}
		
		if((hasLabels & 2) == 2){
			//contain columns labels
			short flag = in.readShort();
			int form = flag>>8;
			int type = flag & 0xff;
			if(form != DATA_FORM.DF_VECTOR.ordinal())
				throw new IOException("The form of matrix columns labels must be vector");
			if(type <0 || type >= types.length)
				throw new IOException("Invalid data type for matrix column labels: " + type);
			columnLabels = (Vector)factory.createEntity(DATA_FORM.DF_VECTOR, types[type], in);
		}
		
		short flag = in.readShort();
		int type = flag & 0xff;
		if(type <0 || type >= types.length)
			throw new IOException("Invalid data type for matrix: " + type);
		rows = in.readInt();
		columns = in.readInt(); 
		readMatrixFromInputStream(rows, columns, in);
	}
	
	protected int getIndex(int row, int column){
		return column * rows + row;
	}
	
	public boolean hasRowLabel(){
		return rowLabels != null;
	}
	
	public boolean hasColumnLabel(){
		return columnLabels != null;
	}
	
	public Scalar getRowLabel(int index){
		return rowLabels.get(index);
	}
	
	public Scalar getColumnLabel(int index){
		return columnLabels.get(index);
	}
	
	public Vector getRowLabels() { 
		return rowLabels;
	}
	
	public void setRowLabels(Vector vector){
		if(vector.rows() != rows)
			throw new IllegalArgumentException("the row label size doesn't equal to the row number of the matrix.");
		rowLabels = vector;
	}
	
	public Vector getColumnLabels(){
		return columnLabels;
	}
	
	public void setColumnLabels(Vector vector){
		if(vector.rows() != columns)
			throw new IllegalArgumentException("the column label size doesn't equal to the column number of the matrix.");
		columnLabels = vector;
	}
	
	public String getString(){
		int rows = Math.min(Utils.DISPLAY_ROWS,rows());
		int limitColMaxWidth=25;
	    int length=0;
	    int curCol=0;
	    int maxColWidth;
		StringBuilder[] list = new StringBuilder[rows+1];
		String[] listTmp = new String[rows+1];
	    int i,curSize;

	    for(i=0; i<list.length; ++i)
	    	list[i] = new StringBuilder();
	    
	    //display row label
	    if(rowLabels != null){
			listTmp[0]="";
			maxColWidth=0;
			for(i=0;i<rows;i++){
				listTmp[i+1]=rowLabels.get(i).getString();
				if(listTmp[i+1].length()>maxColWidth)
					maxColWidth=listTmp[i+1].length();
			}
			maxColWidth++;
			for(i=0;i<=rows;i++){
				curSize=listTmp[i].length();
				if(curSize<=maxColWidth){
					list[i].append(listTmp[i]);
					if(curSize<maxColWidth){
						for(int j=0; j<maxColWidth-curSize; ++j)
							list[i].append(' ');
					}
				}
				else{
					if(maxColWidth>3)
						list[i].append(listTmp[i].substring(0,maxColWidth-3));
					list[i].append("...");
				}
			}
			length+=maxColWidth;
	    }

	    while(length<Utils.DISPLAY_WIDTH && curCol<columns()){
	    	listTmp[0]=columnLabels == null ?"#"+curCol : columnLabels.get(curCol).getString();
	    	maxColWidth=0;
	    	for(i=0;i<rows;i++){
	    		listTmp[i+1]=get(i, curCol).getString();
	    		if(listTmp[i+1].length()>maxColWidth)
	    			maxColWidth=listTmp[i+1].length();
	    	}
	    	if(maxColWidth>limitColMaxWidth)
	    		maxColWidth=limitColMaxWidth;
	    	if((int)listTmp[0].length()>maxColWidth)
	    		maxColWidth=Math.min(limitColMaxWidth, listTmp[0].length());
	    	if(curCol<columns()-1)
	    		maxColWidth++;

	    	if(length+maxColWidth>Utils.DISPLAY_WIDTH && curCol+1<columns())
	    		break;

	    	for(i=0;i<=rows;i++){
	    		curSize=listTmp[i].length();
	    		if(curSize<=maxColWidth){
	    			list[i].append(listTmp[i]);
	    			if(curSize<maxColWidth){
	    				for(int j=0; j<maxColWidth-curSize; ++j)
	    					list[i].append(' ');
	    			}
	    		}
	    		else{
	    			if(maxColWidth>3)
	    				list[i].append(listTmp[i].substring(0,maxColWidth-3));
	    			list[i].append("...");
	    		}
	    	}
	    	length+=maxColWidth;
	    	curCol++;
	    }

	    if(curCol<columns){
	    	for(i=0;i<=rows;i++)
	    		list[i].append("...");
	    }

	    StringBuilder resultStr = new StringBuilder();
	    for(i=0;i<=rows;i++){
	    	resultStr.append(list[i]);
	    	resultStr.append("\n");
	    }
	    if(rows<rows())
	    	resultStr.append("...\n");
	    return resultStr.toString();
	}
	
	@Override
	public DATA_FORM getDataForm() {
		return DATA_FORM.DF_MATRIX;
	}
	
	@Override
	public int rows() {
		return rows;
	}

	@Override
	public int columns() {
		return columns;
	}
	
	public void write(ExtendedDataOutput out) throws IOException{
		int flag = (DATA_FORM.DF_MATRIX.ordinal() << 8) + getDataType().ordinal();
		out.writeShort(flag);
		byte labelFlag = (byte)((hasRowLabel() ? 1 : 0) + (hasColumnLabel() ? 2 : 0));
		out.writeByte(labelFlag);
		if(hasRowLabel())
			rowLabels.write(out);
		if(hasColumnLabel())
			columnLabels.write(out);
		out.writeShort(flag);
		out.writeInt(rows());
		out.writeInt(columns());
		writeVectorToOutputStream(out);
	}
}
