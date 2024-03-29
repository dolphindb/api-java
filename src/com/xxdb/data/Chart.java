package com.xxdb.data;

/**
 * 
 * Interface for chart object
 *
 */

public interface Chart extends Dictionary{
	enum CHART_TYPE {CT_AREA, CT_BAR, CT_COLUMN, CT_HISTOGRAM, CT_LINE, CT_PIE, CT_SCATTER, CT_TREND, CT_KLINE, CT_STACK};
	
	CHART_TYPE getChartType() throws Exception;
	Matrix getData() throws Exception;
	String getTitle();
	String getXAxisName();
	String getYAxisName();
	BasicDictionary getExtraParameters();
	Entity getExtraParameter(CHART_PARAMETER_TYPE key);
}


