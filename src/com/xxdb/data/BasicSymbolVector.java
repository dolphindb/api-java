package com.xxdb.data;

import java.io.IOException;
import java.util.List;
import com.xxdb.io.ExtendedDataInput;
import com.xxdb.io.ExtendedDataOutput;

/**
 * 
 * Corresponds to DolphinDB symbol vector
 *
 */
public class BasicSymbolVector extends AbstractVector {
	private SymbolBase base;
	private int[] values;
	
	public BasicSymbolVector(int size){
		super(DATA_FORM.DF_VECTOR);
		base = new SymbolBase(0);
		values = new int[size];
	}
	
	public BasicSymbolVector(SymbolBase base, int size){
		super(DATA_FORM.DF_VECTOR);
		this.base = base;
		values = new int[size];
	}
	
	public BasicSymbolVector(List<String> list){
		super(DATA_FORM.DF_VECTOR);
		base = new SymbolBase(0);
		values = new int[list.size()];
		for (int i=0; i<list.size(); ++i) {
			if(list.get(i) != null)
				values[i] = base.find(list.get(i), true);
			else
				values[i] = 0;
		}
	}
	
	public BasicSymbolVector(SymbolBase base, int[] values, boolean copy){
		super(DATA_FORM.DF_VECTOR);
		this.base  = base;
		if(copy){
			values = new int[values.length];
			System.arraycopy(this.values, 0, values, 0, values.length);
		}
		else{
			this.values = values;
		}
	}

	protected BasicSymbolVector(DATA_FORM df, ExtendedDataInput in) throws IOException {
		super(df);
		int rows = in.readInt();
		int columns = in.readInt();
		int size = rows * columns;
		values = new int[size];
		base = new SymbolBase(in);
		for (int i = 0; i < size; ++i){
			values[i] = in.readInt();
		}
	}
	
	protected BasicSymbolVector(DATA_FORM df, ExtendedDataInput in, SymbolBaseCollection collection) throws IOException{
		super(df);
		int rows = in.readInt();
		int columns = in.readInt();
		int size = rows * columns;
		values = new int[size];
		base = collection.add(in);
		for (int i = 0; i < size; ++i){
			values[i] = in.readInt();
		}
	}
	
	public Scalar get(int index){
		return new BasicString(base.getSymbol(values[index]));
	}
	
	public Vector getSubVector(int[] indices){
		int length = indices.length;
		int[] sub = new int[length];
		for(int i=0; i<length; ++i)
			sub[i] = values[indices[i]];
		return new BasicSymbolVector(base, sub, false);
	}
	
	@Override
	public String getString(int index){
		return base.getSymbol(values[index]);
	}

	@Override
	public int getUnitLength() {
		return 4;
	}

	public void set(int index, Scalar value) throws Exception {
		values[index] = base.find(value.getString(), true);
	}

	public void setString(int index, String value){
		values[index] = base.find(value, true);
	}
	
	@Override
	public int hashBucket(int index, int buckets){
		return BasicString.hashBucket(base.getSymbol(values[index]), buckets);
	}

	@Override
	public Vector combine(Vector vector) {
		BasicSymbolVector v = (BasicSymbolVector)vector;
		int newSize = this.rows() + v.rows();
		int[] newValue = new int[newSize];
		System.arraycopy(this.values,0, newValue,0,this.rows());
		if(v.base == base)
			System.arraycopy(v.values,0, newValue,this.rows(),v.rows());
		else{
			SymbolBase vBase = v.base;
			int length = vBase.size();
			int[] mapper = new int[length];
			for(int i=0; i<length; ++i)
				mapper[i] = base.find(vBase.getSymbol(i), true);
			length = v.rows();
			int[] vValues = v.values;
			int baseRow = this.rows();
			for(int i=0; i<length; ++i){
				newValue[baseRow + i] = mapper[vValues[i]];
			}
		}
		return new BasicSymbolVector(base, newValue, false);
	}

	@Override
	public boolean isNull(int index) {
		return values[index] == 0;
	}

	@Override
	public void setNull(int index) {
		values[index] = 0;
	}

	@Override
	public DATA_CATEGORY getDataCategory() {
		return Entity.DATA_CATEGORY.LITERAL;
	}

	@Override
	public DATA_TYPE getDataType() {
		return DATA_TYPE.DT_SYMBOL;
	}
	
	@Override
	public Class<?> getElementClass(){
		return BasicString.class;
	}

	@Override
	public void serialize(int start, int count, ExtendedDataOutput out) throws IOException {
		for (int i = 0; i < count; i++){
			out.writeInt(values[start + i]);//todo:Have question
		}
	}

	@Override
	public int rows() {
		return values.length;
	}	
	
	protected void writeVectorToOutputStream(ExtendedDataOutput out) throws IOException{
		base.write(out);
		out.writeIntArray(values);
	}
	
	public void write(ExtendedDataOutput out, SymbolBaseCollection collection) throws IOException{
		int dataType = getDataType().getValue() + 128;
			int flag = (DATA_FORM.DF_VECTOR.ordinal() << 8) + dataType;
		out.writeShort(flag);
		out.writeInt(rows());
		out.writeInt(columns());
		collection.write(out, base);
		out.writeIntArray(values);
	}
	
	@Override
	public int asof(Scalar value) {
		String target = value.getString();
		int start = 0;
		int end = values.length - 1;
		int mid;
		while(start <= end){
			mid = (start + end)/2;
			if(base.getSymbol(values[mid]).compareTo(target) <= 0)
				start = mid + 1;
			else
				end = mid - 1;
		}
		return end;
	}
}
