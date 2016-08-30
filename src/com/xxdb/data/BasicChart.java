package com.xxdb.data;

import java.io.IOException;

import com.xxdb.io.ExtendedDataInput;

public class BasicChart extends BasicDictionary implements Chart{
	private static BasicString KEY_CHARTTYPE = new BasicString("chartType");
	private static BasicString KEY_DATA = new BasicString("data");
	private static BasicString KEY_TITLE = new BasicString("title");
	
	public BasicChart(ExtendedDataInput in) throws IOException{
		super(DATA_TYPE.DT_ANY, in);
	}
	
	public BasicChart(int capacity){
		super(DATA_TYPE.DT_STRING, DATA_TYPE.DT_ANY, capacity);
	}
	
	public BasicChart(){
		this(0);
	}
	
	@Override
	public DATA_FORM getDataForm() {
		return DATA_FORM.DF_CHART;
	}

	@Override
	public CHART_TYPE getChartType() throws Exception{
		Entity chartType = get(KEY_CHARTTYPE);
		if(chartType == null || !chartType.isScalar())
			throw new RuntimeException("Invalid chart object. Chart type is not defined.");
		return CHART_TYPE.values()[((Scalar)chartType).getNumber().intValue()];
	}

	@Override
	public Matrix getData() {
		Entity data = get(KEY_DATA);
		if(data == null || !data.isMatrix())
			throw new RuntimeException("Invalid chart object. Chart data is not set.");
		return (Matrix)data;
	}

	@Override
	public String getTitle() {
		Entity title = get(KEY_TITLE);
		if(title == null || (!title.isScalar() && !title.isVector()))
			return "";
		else if(title.isScalar())
			return title.getString();
		else
			return ((Vector)title).get(0).getString();
	}

	@Override
	public String getXAxisName() {
		Entity title = get(KEY_TITLE);
		if(title == null || !title.isVector() || title.rows()<2)
			return "";
		else
			return ((Vector)title).get(1).getString();
	}

	@Override
	public String getYAxisName() {
		Entity title = get(KEY_TITLE);
		if(title == null || !title.isVector() || title.rows()<3)
			return "";
		else
			return ((Vector)title).get(2).getString();
	}
	
	
}
