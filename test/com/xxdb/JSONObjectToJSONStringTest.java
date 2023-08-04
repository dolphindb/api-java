package com.xxdb;

import com.alibaba.fastjson2.JSONObject;
import com.xxdb.data.*;
import com.xxdb.data.Void;
import org.junit.Test;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;

public class JSONObjectToJSONStringTest {

    @Test
    public void testScalar() {
        BasicBoolean basicBoolean = new BasicBoolean(true);
        String basicBooleanStr = JSONObject.toJSONString(basicBoolean);
        System.out.println(basicBooleanStr);

        BasicByte basicByte = new BasicByte((byte) 13);
        String basicByteStr = JSONObject.toJSONString(basicByte);
        System.out.println(basicByteStr);

        BasicChart basicChart = new BasicChart(5);
//        String basicChartStr = JSONObject.toJSONString(basicChart);
//        System.out.println(basicBooleanStr);

        BasicComplex basicComplex = new BasicComplex(1.1, 2.5);
        String basicComplexStr = JSONObject.toJSONString(basicComplex);
        System.out.println(basicBooleanStr);

        BasicDate basicDate = new BasicDate(LocalDate.of(2022, 2, 2));
        String basicDateStr = JSONObject.toJSONString(basicDate);
        System.out.println(basicBooleanStr);

        BasicDateHour basicDateHour = new BasicDateHour(LocalDateTime.now());
        String basicDateHourStr = JSONObject.toJSONString(basicDateHour);
        System.out.println(basicDateHourStr);

        BasicDateTime basicDateTime = new BasicDateTime(LocalDateTime.of(2000, 7, 29, 11, 07));
        String basicDateTimeStr = JSONObject.toJSONString(basicDateTime);
        System.out.println(basicDateTimeStr);

        BasicDecimal32 basicDecimal32 = new BasicDecimal32("1.23333", 3);
        String basicDecimal32Str = JSONObject.toJSONString(basicDecimal32);
        System.out.println(basicDecimal32Str);

        BasicDecimal64 decimal64 = new BasicDecimal64("1.2333", 3);
        String decimal64Str = JSONObject.toJSONString(decimal64);
        System.out.println(decimal64Str);

        BasicDecimal128 decimal128 = new BasicDecimal128("1.23333", 4);
        String decimal128Str = JSONObject.toJSONString(decimal128);
        System.out.println(decimal128Str);

        BasicDictionary basicDictionary = new BasicDictionary(Entity.DATA_TYPE.DT_STRING, Entity.DATA_TYPE.DT_DICTIONARY, 1);
        String basicDictionaryStr = JSONObject.toJSONString(basicDictionary);
        System.out.println(basicDictionaryStr);

        BasicDouble basicDouble = new BasicDouble(1.23333);
        String basicDoubleStr = JSONObject.toJSONString(basicDouble);
        System.out.println(basicDoubleStr);

        BasicDuration basicDuration = new BasicDuration(Entity.DURATION.SECOND, -5);
        String basicDurationStr = JSONObject.toJSONString(basicDuration);
        System.out.println(basicBooleanStr);

        BasicFloat basicFloat = new BasicFloat(1.23333f);
        String basicFloatStr = JSONObject.toJSONString(basicFloat);
        System.out.println(basicFloatStr);

        BasicInt basicInt = new BasicInt(1);
        String basicIntStr = JSONObject.toJSONString(basicInt);
        System.out.println(basicIntStr);

        BasicInt128 basicInt128 = new BasicInt128(456, 456);
        String basicInt128Str = JSONObject.toJSONString(basicInt128);
        System.out.println(basicInt128Str);

        BasicIPAddr basicIPAddr = new BasicIPAddr(321324, 32433);
        String basicIPAddrStr = JSONObject.toJSONString(basicIPAddr);
        System.out.println(basicIPAddrStr);

        BasicLong basicLong = new BasicLong(111112222);
        String basicLongStr = JSONObject.toJSONString(basicLong);
        System.out.println(basicLongStr);

        BasicMinute basicMinute = new BasicMinute(LocalTime.of(11, 40, 53));
        String basicMinuteStr = JSONObject.toJSONString(basicMinute);
        System.out.println(basicMinuteStr);

        BasicMonth basicMonth = new BasicMonth(2010, Month.APRIL);
        String basicMonthStr = JSONObject.toJSONString(basicMonth);
        System.out.println(basicMonthStr);

        BasicNanoTime basicNanoTime = new BasicNanoTime(LocalDateTime.of(2000, 2, 2, 3, 2, 3, 2));
        String basicNanoTimeStr = JSONObject.toJSONString(basicNanoTime);
        System.out.println(basicNanoTimeStr);

        BasicNanoTimestamp basicNanoTimestamp = new BasicNanoTimestamp(LocalDateTime.of(2000, 7, 29, 11, 07));
        String basicNanoTimestampStr = JSONObject.toJSONString(basicNanoTimestamp);
        System.out.println(basicNanoTimestampStr);

        BasicPoint basicPoint = new BasicPoint(6.4, 9.2);
        String basicPointStr = JSONObject.toJSONString(basicPoint);
        System.out.println(basicPointStr);

        BasicSecond basicSecond = new BasicSecond(LocalTime.of(2, 2, 2));
        String basicSecondStr = JSONObject.toJSONString(basicSecond);
        System.out.println(basicSecondStr);

        BasicSet basicSet = new BasicSet(Entity.DATA_TYPE.DT_INT, 4);
        String basicSetStr = JSONObject.toJSONString(basicSet);
        System.out.println(basicSetStr);

        BasicShort basicShort = new BasicShort((short) 35);
        String basicShortStr = JSONObject.toJSONString(basicShort);
        System.out.println(basicShortStr);

        BasicString basicString = new BasicString("11111", true);
//        String basicStringStr = JSONObject.toJSONString(basicString);
//        System.out.println(basicString);

        BasicTime basicTime = new BasicTime(LocalTime.of(13, 7, 55));
        String basicTimeStr = JSONObject.toJSONString(basicTime);
        System.out.println(basicTimeStr);

        BasicTimestamp basicTimestamp = new BasicTimestamp(LocalDateTime.of(2022, 2, 2, 1, 1, 1, 1));
        String basicTimestampStr = JSONObject.toJSONString(basicTimestamp);
        System.out.println(basicTimestampStr);

        BasicUuid basicUuid = new BasicUuid(321324, 32433);
        String basicUuidStr = JSONObject.toJSONString(basicUuid);
        System.out.println(basicUuidStr);

        SymbolBase symbolBase = new SymbolBase(0);
        String symbolBaseStr = JSONObject.toJSONString(symbolBase);
        System.out.println(symbolBaseStr);

        com.xxdb.data.Void aVoid = new Void();
        String aVoidStr = JSONObject.toJSONString(aVoid);
        System.out.println(aVoidStr);
    }

    @Test
    public void testVector() {
        BasicBooleanVector basicBooleanVector = new BasicBooleanVector(6);
        String basicBooleanVectorStr = JSONObject.toJSONString(basicBooleanVector);
        System.out.println(basicBooleanVectorStr);

        BasicByteVector basicByteVector = new BasicByteVector(6);
        String basicByteVectorStr = JSONObject.toJSONString(basicByteVector);
        System.out.println(basicByteVectorStr);

        BasicComplexVector basicComplexVector = new BasicComplexVector(6);
        String basicComplexVectorStr = JSONObject.toJSONString(basicComplexVector);
        System.out.println(basicComplexVectorStr);

        BasicDateHourVector basicDateHourVector = new BasicDateHourVector(6);
        String basicDateHourVectorStr = JSONObject.toJSONString(basicDateHourVector);
        System.out.println(basicDateHourVectorStr);

        BasicDateTimeVector basicDateTimeVector = new BasicDateTimeVector(6);
        String basicDateTimeVectorStr = JSONObject.toJSONString(basicDateTimeVector);
        System.out.println(basicDateTimeVectorStr);

        BasicDecimal32Vector basicDecimal32Vector = new BasicDecimal32Vector(6);
        String basicDecimal32VectorStr = JSONObject.toJSONString(basicDecimal32Vector);
        System.out.println(basicDecimal32VectorStr);

        BasicDecimal64Vector basicDecimal64Vector = new BasicDecimal64Vector(6);
        String basicDecimal64VectorStr = JSONObject.toJSONString(basicDecimal64Vector);
        System.out.println(basicDecimal64VectorStr);

        BasicDecimal128Vector basicDecimal128Vector = new BasicDecimal128Vector(new String[]{"0.0", "-123.00432", "132.204234", "100.0"}, 4);
        String basicDecimal128VectorStr = JSONObject.toJSONString(basicDecimal128Vector);
        System.out.println(basicDecimal128VectorStr);

        BasicDoubleVector basicDoubleVector = new BasicDoubleVector(6);
        String basicDoubleVectorStr = JSONObject.toJSONString(basicDoubleVector);
        System.out.println(basicDoubleVectorStr);

        BasicDurationVector basicDurationVector = new BasicDurationVector(6);
        String basicDurationVectorStr = JSONObject.toJSONString(basicDurationVector);
        System.out.println(basicDurationVectorStr);

        BasicFloatVector basicFloatVector = new BasicFloatVector(6);
        String basicFloatVectorStr = JSONObject.toJSONString(basicFloatVector);
        System.out.println(basicFloatVectorStr);

        BasicInt128Vector basicInt128Vector = new BasicInt128Vector(6);
        String basicInt128VectorStr = JSONObject.toJSONString(basicInt128Vector);
        System.out.println(basicInt128VectorStr);

        BasicIntVector basicIntVector = new BasicIntVector(6);
        String basicIntVectorStr = JSONObject.toJSONString(basicIntVector);
        System.out.println(basicIntVectorStr);

        BasicIPAddrVector basicIPAddrVector = new BasicIPAddrVector(6);
        String basicIPAddrVectorStr = JSONObject.toJSONString(basicIPAddrVector);
        System.out.println(basicIPAddrVectorStr);

        BasicLongVector basicLongVector = new BasicLongVector(6);
        String basicLongVectorStr = JSONObject.toJSONString(basicLongVector);
        System.out.println(basicLongVectorStr);

        BasicMinuteVector basicMinuteVector = new BasicMinuteVector(6);
        String basicMinuteVectorStr = JSONObject.toJSONString(basicMinuteVector);
        System.out.println(basicMinuteVectorStr);

        BasicMonthVector basicMonthVector = new BasicMonthVector(6);
        String basicMonthVectorStr = JSONObject.toJSONString(basicMonthVector);
        System.out.println(basicMonthVectorStr);

        BasicNanoTimestampVector basicNanoTimestampVector = new BasicNanoTimestampVector(6);
        String basicNanoTimestampVectorStr = JSONObject.toJSONString(basicNanoTimestampVector);
        System.out.println(basicNanoTimestampVectorStr);

        BasicNanoTimeVector basicNanoTimeVector = new BasicNanoTimeVector(6);
        String basicNanoTimeVectorStr = JSONObject.toJSONString(basicNanoTimeVector);
        System.out.println(basicNanoTimeVectorStr);

        BasicPointVector basicPointVector = new BasicPointVector(6);
        String basicPointVectorStr = JSONObject.toJSONString(basicPointVector);
        System.out.println(basicPointVectorStr);

        BasicSecondVector basicSecondVector = new BasicSecondVector(6);
        String basicSecondVectorStr = JSONObject.toJSONString(basicSecondVector);
        System.out.println(basicSecondVectorStr);

        BasicShortVector basicShortVector = new BasicShortVector(6);
        String basicShortVectorStr = JSONObject.toJSONString(basicShortVector);
        System.out.println(basicShortVectorStr);

        BasicStringVector basicStringVector = new BasicStringVector(6);
        String basicStringVectorStr = JSONObject.toJSONString(basicStringVector);
        System.out.println(basicStringVectorStr);

        SymbolBase base = new SymbolBase(2);
//        BasicSymbolVector basicSymbolVector = new BasicSymbolVector(base,4);
//        String basicSymbolVectorStr = JSONObject.toJSONString(basicSymbolVector);
//        System.out.println(basicSymbolVectorStr);

        BasicTimestampVector basicTimestampVector = new BasicTimestampVector(6);
        String basicTimestampVectorStr = JSONObject.toJSONString(basicTimestampVector);
        System.out.println(basicTimestampVectorStr);

        BasicTimeVector BasicTimeVector = new BasicTimeVector(6);
        String BasicTimeVectorStr = JSONObject.toJSONString(BasicTimeVector);
        System.out.println(BasicTimeVectorStr);

        BasicUuidVector basicUuidVector = new BasicUuidVector(6);
        String basicUuidVectorStr = JSONObject.toJSONString(basicUuidVector);
        System.out.println(basicUuidVectorStr);

        BasicVoidVector basicVoidVector = new BasicVoidVector(6);
        String basicVoidVectorStr = JSONObject.toJSONString(basicVoidVector);
        System.out.println(basicVoidVectorStr);
    }

    @Test
    public void testMatrix() {
        BasicBooleanMatrix basicBooleanMatrix = new BasicBooleanMatrix(2, 2);
        String basicBooleanMatrixStr = JSONObject.toJSONString(basicBooleanMatrix);
        System.out.println(basicBooleanMatrixStr);

        BasicByteMatrix basicByteMatrix = new BasicByteMatrix(2, 2);
        String basicByteMatrixStr = JSONObject.toJSONString(basicByteMatrix);
        System.out.println(basicByteMatrixStr);

        BasicComplexMatrix BasicComplexMatrix = new BasicComplexMatrix(2, 2);
//        String BasicComplexMatrixStr = JSONObject.toJSONString(BasicComplexMatrix);
//        System.out.println(BasicComplexMatrixStr);

        BasicDateHourMatrix basicDateHourMatrix = new BasicDateHourMatrix(2, 2);
        String basicDateHourMatrixStr = JSONObject.toJSONString(basicDateHourMatrix);
        System.out.println(basicDateHourMatrixStr);

        BasicDateTimeMatrix basicDateTimeMatrix = new BasicDateTimeMatrix(2, 2);
        String basicDateTimeMatrixStr = JSONObject.toJSONString(basicDateTimeMatrix);
        System.out.println(basicDateTimeMatrixStr);

        BasicDoubleMatrix basicDoubleMatrix = new BasicDoubleMatrix(2, 2);
        String basicDoubleMatrixStr = JSONObject.toJSONString(basicDoubleMatrix);
        System.out.println(basicDoubleMatrixStr);

        BasicFloatMatrix basicFloatMatrix = new BasicFloatMatrix(2, 2);
        String basicFloatMatrixStr = JSONObject.toJSONString(basicFloatMatrix);
        System.out.println(basicFloatMatrixStr);

        BasicIntMatrix basicIntMatrix = new BasicIntMatrix(2, 2);
        String basicIntMatrixStr = JSONObject.toJSONString(basicIntMatrix);
        System.out.println(basicIntMatrixStr);

        BasicLongMatrix basicLongMatrix = new BasicLongMatrix(2, 3);
        String basicLongMatrixStr = JSONObject.toJSONString(basicLongMatrix);
        System.out.println(basicLongMatrixStr);

        BasicMinuteMatrix basicMinuteMatrix = new BasicMinuteMatrix(2, 3);
        String basicMinuteMatrixStr = JSONObject.toJSONString(basicMinuteMatrix);
        System.out.println(basicMinuteMatrixStr);

        BasicMonthMatrix basicMonthMatrix = new BasicMonthMatrix(2, 2);
        String basicMonthMatrixStr = JSONObject.toJSONString(basicMonthMatrix);
        System.out.println(basicMonthMatrixStr);

        BasicNanoTimeMatrix basicNanoTimeMatrix = new BasicNanoTimeMatrix(2, 3);
        String basicNanoTimeMatrixStr = JSONObject.toJSONString(basicNanoTimeMatrix);
        System.out.println(basicNanoTimeMatrixStr);

        BasicNanoTimestampMatrix basicNanoTimestampMatrix = new BasicNanoTimestampMatrix(2, 3);
        String basicNanoTimestampMatrixStr = JSONObject.toJSONString(basicNanoTimestampMatrix);
        System.out.println(basicNanoTimestampMatrixStr);

        BasicSecondMatrix basicSecondMatrix = new BasicSecondMatrix(2, 3);
        String basicSecondMatrixStr = JSONObject.toJSONString(basicSecondMatrix);
        System.out.println(basicSecondMatrixStr);

        BasicShortMatrix basicShortMatrix = new BasicShortMatrix(2, 2);
        String basicShortMatrixStr = JSONObject.toJSONString(basicShortMatrix);
        System.out.println(basicShortMatrixStr);

        BasicStringMatrix basicStringMatrix = new BasicStringMatrix(2, 2);
//        String basicStringMatrixStr = JSONObject.toJSONString(basicStringMatrix);
//        System.out.println(basicStringMatrixStr);

        BasicTimeMatrix basicTimeMatrix = new BasicTimeMatrix(2, 2);
        String basicTimeMatrixStr = JSONObject.toJSONString(basicTimeMatrix);
        System.out.println(basicTimeMatrixStr);

        BasicTimestampMatrix basicTimestampMatrix = new BasicTimestampMatrix(2, 2);
        String basicTimestampMatrixStr = JSONObject.toJSONString(basicTimestampMatrix);
        System.out.println(basicTimestampMatrixStr);


    }

}
