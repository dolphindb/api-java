package com.xxdb.data;

import java.io.IOException;
import java.util.*;
import com.xxdb.compression.VectorDecompressor;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

/**
 * 
 * Corresponds to DolphinDB table object
 *
 */

public class BasicTable extends AbstractEntity implements Table{
	private List<Vector> columns = new ArrayList<Vector>();
	private List<String> colNames = new ArrayList<String>();
	private Map<String, Integer> colNamesIndex = new HashMap<String, Integer>();
	private int[] colCompresses = null;
	private int colRows;

	public BasicTable(ExtendedDataInput in) throws IOException{
		int rows = in.readInt();
		int cols = in.readInt();
		String tableName = in.readString(); 
		
		//read column names
		for(int i=0; i<cols; ++i){
			String name = in.readString();
			colNamesIndex.put(name, colNamesIndex.size());
			colNames.add(name);
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
				throw new IOException("Invalid form for column [" + colNames.get(i) + "] for table " + tableName);
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
				throw new IOException("The number of rows for column " + colNames.get(i) + " is not consistent with other columns");
			this.colRows = rows;
			columns.add(vector);
		}
		if(collection != null)
			collection.clear();
	}
	
    public BasicTable(final List<String> colNames, final List<Vector> cols) {
		if(colNames.size() != cols.size()){
			throw new Error("The length of column name and column data is unequal.");
		}

		this.colRows = cols.get(0).rows();
		for (int i=0;i<cols.size();i++) {
			Vector v = cols.get(i);
			if(v.rows() != this.colRows)
				throw new Error("The length of column " + colNames.get(i) + "  must be the same as the first column length.");
		}
        this.setColName(colNames);
        this.setColumns(cols);
    }

	public void setColumnCompressTypes(int[] colCompresses) {
		if (colCompresses!=null && colCompresses.length != columns.size()) {
			throw new RuntimeException("Compress type size must match column size "+ columns.size()+".");
		}
		if(colCompresses!=null) {
			for (int i = 0; i < colCompresses.length; i++) {
				if (colCompresses[i] == Vector.COMPRESS_DELTA) {
					Vector column=getColumn(i);
					DATA_TYPE dataType = column.getDataType();
					if(column.getDataCategory() != DATA_CATEGORY.TEMPORAL) {
						if (dataType != DATA_TYPE.DT_SHORT && dataType != DATA_TYPE.DT_INT && dataType != DATA_TYPE.DT_LONG) {
							throw new RuntimeException("Delta compression only supports short/int/long and temporal data.");
						}
					}
					if(dataType.getValue() >= Entity.DATA_TYPE.DT_BOOL_ARRAY.getValue() && dataType.getValue() <= Entity.DATA_TYPE.DT_POINT_ARRAY.getValue()){
						throw new RuntimeException("Delta compression doesn't support array vector.");
					}
				}
			}
		}
		if(colCompresses!=null){
			this.colCompresses =new int[colCompresses.length];
			System.arraycopy(colCompresses,0, this.colCompresses,0,colCompresses.length);
		}else
			this.colCompresses = null;
	}

	/**
	 * set columns' names
	 * @param colNames
	 */
	public void setColName (final List<String> colNames) {
        this.colNames.clear();
		colNamesIndex.clear();
        for (String name : colNames){
			this.colNames.add(name);
			colNamesIndex.put(name, colNamesIndex.size());
		}
    }

	/**
	 * replace a specific colName.
	 * @param originalColName
	 * @param newColName
	 */
	public void replaceColName(String originalColName, String newColName) {
		if (Utils.isEmpty(originalColName) || Utils.isEmpty(newColName))
			throw new RuntimeException("The param 'newColName' cannot be null or empty.");

		if (!this.colNames.contains(originalColName) && this.colNames.contains(newColName))
			throw new RuntimeException("The newColName '" + newColName +"' already exists in table. Column names cannot be duplicated.");

		if (this.colNames.contains(originalColName)) {
			int index = colNames.indexOf(originalColName);
			colNames.set(index, newColName);
		} else {
			throw new RuntimeException("The param originalColName '" + originalColName +"' does not exist in table.");
		}
	}
    
    public void setColumns (final List<Vector> cols) {
    	columns.clear();
        // this is a shallow copy!
        for (Vector vector : cols)
        	columns.add(vector);
        
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
			return this.colRows;
	}

	@Override
	public int columns() {
		return columns.size();
	}

	@Override
	public Vector getColumn(int index) {
		return columns.get(index);
	}

	@Override
	public Vector getColumn(String name) {
		Integer index = colNamesIndex.get(name);
		if(index == null)
			return null;
		else
			return getColumn(index);
	}
	
	public String getColumnName(int index){
		return colNames.get(index);
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
				listTmp[i+1]=getColumn(curCol).getString(i);
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
		try {
			if(rowIndex<rows()){
				jsonStr.append("{");
				for(int i = 0; i< colNames.size(); i++){
					jsonStr.append("\"");
					jsonStr.append(colNames.get(i));
					jsonStr.append("\":");
					if (columns.get(i) instanceof BasicDoubleVector || columns.get(i) instanceof BasicFloatVector)
						jsonStr.append(((Scalar) columns.get(i).get(rowIndex)).isNull() ? "null" : ((Scalar) columns.get(i).get(rowIndex)).getJsonString());
					else if (columns.get(i).getDataType().getValue() >= 65)
						jsonStr.append(columns.get(i).getJsonString(rowIndex));
					else
						jsonStr.append(((Scalar) columns.get(i).get(rowIndex)).getJsonString());
					if(i< colNames.size()-1)
						jsonStr.append(",");
				}
				jsonStr.delete(jsonStr.length()-1,jsonStr.length()-1);
				jsonStr.append("}");
			}
		}catch (Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		return jsonStr.toString();
	}

	public void write(ExtendedDataOutput out) throws IOException{
		int flag = (DATA_FORM.DF_TABLE.ordinal() << 8) + getDataType().getValue();
		out.writeShort(flag);
		out.writeInt(rows());
		out.writeInt(columns());
		out.writeString(""); //table name
		for(String colName : colNames)
			out.writeString(colName);
		SymbolBaseCollection collection = null;
		for(Vector vector : columns){
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
			else {
				if(colCompresses !=null){
					v.setCompressedMethod(colCompresses[i]);
				}
				v.writeCompressed(output);
			}
		}

	}

	public BasicTable combine(BasicTable table){
		List<Vector> newCol = new ArrayList<>();
		for (int i=0; i< this.columns();i++) {
			newCol.add(this.getColumn(i).combine(table.getColumn(i)));
		}
		return new BasicTable(this.colNames,newCol);
	}
	
	public Table getSubTable(int[] indices){
		int colCount = columns.size();
		List<Vector> cols = new ArrayList<Vector>(colCount);
		for(int i=0; i<colCount; ++i)
			cols.add(columns.get(i).getSubVector(indices));
		return new BasicTable(colNames, cols);
	}

	public Table getSubTable(int startRow, int endRow){
		int colCount = columns.size();
		List<Vector> cols = new ArrayList<>();
		for (int i = 0; i < colCount; i++){
			int index = startRow;
			int[] indices = new int[endRow - startRow+1];
			for (int j = 0; j < indices.length; j++){
				indices[j] = index;
				index++;
			}
			cols.add(columns.get(i).getSubVector(indices));
		}
		return new BasicTable(colNames, cols);
	}

	@Override
	public void addColumn(String colName, Vector col) {
		if (Objects.isNull(colName) || Objects.isNull(col))
			throw new RuntimeException("The param 'colName' or 'col' in table cannot be null.");

		if (colName.isEmpty())
			throw new RuntimeException("The param 'colName' cannot be empty.");

		if (colNames.contains(colName))
			throw new RuntimeException("The table already contains column '" + colName + "'.");

		if (this.colRows != 0 && col.rows() != this.colRows)
			throw new RuntimeException("The length of column " + colName + "  must be the same as the first column length: " + this.colRows +".");

		colNames.add(colName);
		colNamesIndex.put(colName, colNamesIndex.size());
		columns.add(col);
		this.colRows = col.rows();
	}

	@Override
	public void replaceColumn(String colName, Vector col) {
		if (Objects.isNull(colName) || Objects.isNull(col))
			throw new RuntimeException("The param 'colName' or 'col' in table cannot be null.");

		if (!colNames.contains(colName))
			throw new RuntimeException("The column '" + colName + "' to be replaced doesn't exist in the table.");

		if (this.colRows != 0 && col.rows() != this.colRows)
			throw new RuntimeException("The length of column " + colName + "  must be the same as the first column length: " + this.colRows +".");

		colNames.add(colName);
		colNamesIndex.put(colName, colNamesIndex.size());
		columns.add(col);
		this.colRows = col.rows();
	}
}
