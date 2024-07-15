package com.xxdb.route;

import com.xxdb.DBConnection;
import com.xxdb.data.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class AutoFitTableAppender {
    enum APPEND_ACTION {fitColumnType}
    String dbUrl_;
    String tableName_;
    boolean async_;
    DBConnection con_;
    APPEND_ACTION _action;

    private static final Logger log = LoggerFactory.getLogger(AutoFitTableAppender.class);

    public AutoFitTableAppender(String dbUrl, String tableName, DBConnection conn) {
        this.dbUrl_ = dbUrl;
        this.tableName_ = tableName;
        this.con_ = conn;
        this._action = APPEND_ACTION.fitColumnType;
    }

    public AutoFitTableAppender(String dbUrl, String tableName, DBConnection conn, APPEND_ACTION action) {
        this.dbUrl_ = dbUrl;
        this.tableName_ = tableName;
        this.con_ = conn;
        this._action = action;
    }

    public Entity append(BasicTable table) {
        Entity res = null;
        try {
            String runScript;
            if(Objects.equals(dbUrl_, ""))
                runScript="schema(" + tableName_ + ")";
            else
                runScript="schema(loadTable(\"" + dbUrl_ + "\",\"" + tableName_ + "\"))";

            Entity schema = con_.run(runScript);
            int columns = table.columns();
            BasicTable colDefs = (BasicTable) ((BasicDictionary) schema).get(new BasicString("colDefs"));
            BasicStringVector typeList = (BasicStringVector) colDefs.getColumn("typeString");
            BasicStringVector nameList = (BasicStringVector) colDefs.getColumn("name");
            List<String> colName =  Arrays.asList(nameList.getValues());
            List<Vector> cols = new ArrayList<>();

            int rowSize = table.rows();
            for (int i = 0; i < columns; ++i) {
                String name = nameList.getString(i);
                String dstType = typeList.get(i).getString();
                Vector colOrigin = table.getColumn(name);
                Vector dstVector;
                switch (dstType) {
                    case "DATE": {
                        int[] buffer = new int[rowSize];

                        switch (colOrigin.getDataType()) {
                            case DT_DATE: {
                                dstVector = (BasicDateVector) colOrigin;
                                break;
                            }
                            case DT_DATETIME: {
                                for (int j = 0; j < rowSize; ++j) {
                                    buffer[j] = ((BasicDateTimeVector) colOrigin).getInt(j) / 86400;
                                }
                                dstVector = new BasicDateVector(buffer);
                                break;
                            }
                            case DT_TIMESTAMP: {
                                for (int j = 0; j < rowSize; ++j) {
                                    buffer[j] = (int) (((BasicTimestampVector) colOrigin).getLong(j) / 86400000);
                                }
                                dstVector = new BasicDateVector(buffer);
                                break;
                            }
                            case DT_NANOTIMESTAMP: {
                                for (int j = 0; j < rowSize; ++j) {
                                    buffer[j] = (int) (((BasicNanoTimestampVector) colOrigin).getLong(j) / 86400000000000L);
                                }
                                dstVector = new BasicDateVector(buffer);
                                break;
                            }
                            case DT_DATEHOUR: {
                                for (int j = 0; j < rowSize; ++j) {
                                    buffer[j] = (int) (((BasicDateHourVector) colOrigin).getInt(j) / 24);
                                }
                                dstVector = new BasicDateVector(buffer);
                                break;
                            }
                            default:
                                throw new InterruptedException("Can't convert from " + dstType + " into " + getDTString(colOrigin.getDataType()));
                        }
                        break;
                    }
                    case "MONTH": {
                        int[] buffer = new int[rowSize];
                        switch (colOrigin.getDataType()) {
                            case DT_DATE: {
                                for (int j = 0; j < rowSize; ++j) {
                                    int tmp = ((BasicDateVector) colOrigin).getInt(j);
                                    LocalDate localTmp = Utils.parseDate(tmp);
                                    buffer[j] = localTmp.getYear()*12 + localTmp.getMonthValue() - 1;
                                }
                                dstVector = new BasicMonthVector(buffer);
                                break;
                            }
                            case DT_DATETIME: {
                                for (int j = 0; j < rowSize; ++j) {
                                    int tmp = ((BasicDateTimeVector) colOrigin).getInt(j) / 86400;
                                    LocalDate localTmp = Utils.parseDate(tmp);
                                    buffer[j] = localTmp.getYear()*12 + localTmp.getMonthValue() - 1;
                                }
                                dstVector = new BasicMonthVector(buffer);
                                break;
                            }
                            case DT_DATEHOUR: {
                                for (int j = 0; j < rowSize; ++j) {
                                    int tmp = ((BasicDateHourVector) colOrigin).getInt(j) / 24;
                                    LocalDate localTmp = Utils.parseDate(tmp);
                                    buffer[j] = localTmp.getYear()*12 + localTmp.getMonthValue() - 1;
                                }
                                dstVector = new BasicMonthVector(buffer);
                                break;
                            }
                            case DT_MONTH: {
                                dstVector = colOrigin;
                                break;
                            }
                            case DT_TIMESTAMP: {
                                for (int j = 0; j < rowSize; ++j) {
                                    int tmp = (int) (((BasicTimestampVector) colOrigin).getLong(j) / 86400000);
                                    LocalDate localTmp = Utils.parseDate(tmp);
                                    buffer[j] = localTmp.getYear()*12 + localTmp.getMonthValue() - 1;
                                }
                                dstVector = new BasicMonthVector(buffer);
                                break;
                            }
                            case DT_NANOTIMESTAMP: {
                                for (int j = 0; j < rowSize; ++j) {
                                    int tmp = (int) (((BasicNanoTimestampVector) colOrigin).getLong(j) / 86400000000000L);
                                    LocalDate localTmp = Utils.parseDate(tmp);
                                    buffer[j] = localTmp.getYear()*12 + localTmp.getMonthValue() - 1;
                                }
                                dstVector = new BasicMonthVector(buffer);
                                break;
                            }
                            default:
                                throw new InterruptedException("Can't convert from " + dstType + " into " + getDTString(colOrigin.getDataType()));
                        }
                        break;
                    }
                    case "TIME": {
                        int[] buffer = new int[rowSize];
                        switch (colOrigin.getDataType()) {
                            case DT_TIME: {
                                dstVector = colOrigin;
                                break;
                            }
                            case DT_NANOTIME: {
                                for (int j = 0; j < rowSize; ++j) {
                                    buffer[j] = (int) (((BasicNanoTimeVector) colOrigin).getLong(j) / 1000000);
                                }
                                dstVector = new BasicTimeVector(buffer);
                                break;
                            }
                            case DT_TIMESTAMP: {
                                for (int j = 0; j < rowSize; ++j) {
                                    buffer[j] = (int) (((BasicTimestampVector) colOrigin).getLong(j) % 86400000);
                                }
                                dstVector = new BasicTimeVector(buffer);
                                break;
                            }
                            case DT_NANOTIMESTAMP: {
                                for (int j = 0; j < rowSize; ++j) {
                                    buffer[j] = (int) (((BasicNanoTimestampVector) colOrigin).getLong(j) % 86400000000000L / 1000000);
                                }
                                dstVector = new BasicTimeVector(buffer);
                                break;
                            }
                            default:
                                throw new InterruptedException("Can't convert from " + dstType + " into " + getDTString(colOrigin.getDataType()));
                        }
                        break;
                    }
                    case "MINUTE": {
                        int[] buffer = new int[rowSize];
                        switch (colOrigin.getDataType()) {
                            case DT_TIME: {
                                for (int j = 0; j < rowSize; ++j) {
                                    buffer[j] = (int) (((BasicTimeVector) colOrigin).getInt(j) / 60000);
                                }
                                dstVector = new BasicMinuteVector(buffer);
                                break;
                            }
                            case DT_SECOND: {
                                for (int j = 0; j < rowSize; ++j) {
                                    buffer[j] = ((BasicSecondVector) colOrigin).getInt(j) / 60;
                                }
                                dstVector = new BasicMinuteVector(buffer);
                                break;
                            }
                            case DT_MINUTE: {
                                dstVector = colOrigin;
                                break;
                            }
                            case DT_DATETIME: {
                                for (int j = 0; j < rowSize; ++j) {
                                    buffer[j] = ((BasicDateTimeVector) colOrigin).getInt(j) % 86400 / 60;
                                }
                                dstVector = new BasicMinuteVector(buffer);
                                break;
                            }
                            case DT_NANOTIME: {
                                for (int j = 0; j < rowSize; ++j) {
                                    buffer[j] = (int) (((BasicNanoTimeVector) colOrigin).getLong(j) / 60000000000L);
                                }
                                dstVector = new BasicMinuteVector(buffer);
                                break;
                            }
                            case DT_TIMESTAMP: {
                                for (int j = 0; j < rowSize; ++j) {
                                    buffer[j] = (int) (((BasicTimestampVector) colOrigin).getLong(j) / 60000 % 1440);
                                }
                                dstVector = new BasicMinuteVector(buffer);
                                break;
                            }
                            case DT_NANOTIMESTAMP: {
                                for (int j = 0; j < rowSize; ++j) {
                                    buffer[j] = (int) (((BasicNanoTimestampVector) colOrigin).getLong(j) / 60000000000L % 1440);
                                }
                                dstVector = new BasicMinuteVector(buffer);
                                break;
                            }
                            default:
                                throw new InterruptedException("Can't convert from " + dstType + " into " + getDTString(colOrigin.getDataType()));
                        }
                        break;
                    }
                    case "SECOND": {
                        int[] buffer = new int[rowSize];
                        switch (colOrigin.getDataType()) {
                            case DT_TIME: {
                                for (int j = 0; j < rowSize; ++j) {
                                    buffer[j] = ((BasicTimeVector) colOrigin).getInt(j) / 1000;
                                }
                                dstVector = new BasicSecondVector(buffer);
                                break;
                            }
                            case DT_SECOND: {
                                dstVector = colOrigin;
                                break;
                            }
                            case DT_DATETIME: {
                                for (int j = 0; j < rowSize; ++j) {
                                    buffer[j] = ((BasicDateTimeVector) colOrigin).getInt(j) % 86400;
                                }
                                dstVector = new BasicSecondVector(buffer);
                                break;
                            }
                            case DT_NANOTIME: {
                                for (int j = 0; j < rowSize; ++j) {
                                    buffer[j] = (int) (((BasicNanoTimeVector) colOrigin).getLong(j) / 1000000000);
                                }
                                dstVector = new BasicSecondVector(buffer);
                                break;
                            }
                            case DT_TIMESTAMP: {
                                for (int j = 0; j < rowSize; ++j) {
                                    buffer[j] = (int) (((BasicTimestampVector) colOrigin).getLong(j) / 1000 % 86400);
                                }
                                dstVector = new BasicSecondVector(buffer);
                                break;
                            }
                            case DT_NANOTIMESTAMP: {
                                for (int j = 0; j < rowSize; ++j) {
                                    buffer[j] = (int) (((BasicNanoTimestampVector) colOrigin).getLong(j) / 1000000000 % 86400);
                                }
                                dstVector = new BasicSecondVector(buffer);
                                break;
                            }
                            default:
                                throw new InterruptedException("Can't convert from " + dstType + " into " + getDTString(colOrigin.getDataType()));
                        }
                        break;
                    }
                    case "DATETIME": {
                        int[] buffer = new int[rowSize];
                        switch (colOrigin.getDataType()) {
                            case DT_DATETIME: {
                                dstVector = colOrigin;
                                break;
                            }
                            case DT_TIMESTAMP: {
                                for (int j = 0; j < rowSize; ++j) {
                                    buffer[j] = (int) (((BasicTimestampVector) colOrigin).getLong(j) / 1000);
                                }
                                dstVector = new BasicDateTimeVector(buffer);
                                break;
                            }
                            case DT_NANOTIMESTAMP: {
                                for (int j = 0; j < rowSize; ++j) {
                                    buffer[j] = (int) (((BasicNanoTimestampVector) colOrigin).getLong(j) / 1000000000);
                                }
                                dstVector = new BasicDateTimeVector(buffer);
                                break;
                            }
                            default:
                                throw new InterruptedException("Can't convert from " + dstType + " into " + getDTString(colOrigin.getDataType()));
                        }
                        break;
                    }
                    case "TIMESTAMP": {
                        long[] buffer = new long[rowSize];
                        switch (colOrigin.getDataType()) {
                            case DT_TIMESTAMP: {
                                dstVector = colOrigin;
                                break;
                            }
                            case DT_NANOTIMESTAMP: {
                                for (int j = 0; j < rowSize; ++j) {
                                    buffer[j] =((BasicNanoTimestampVector) colOrigin).getLong(j) / 1000000;
                                }
                                dstVector = new BasicTimestampVector(buffer);
                                break;
                            }
                            default:
                                throw new InterruptedException("Can't convert from " + dstType + " into " + getDTString(colOrigin.getDataType()));
                        }
                        break;
                    }
                    case "NANOTIME": {
                        long[] buffer = new long[rowSize];
                        switch (colOrigin.getDataType()) {
                            case DT_NANOTIME: {
                                dstVector = colOrigin;
                                break;
                            }
                            case DT_NANOTIMESTAMP: {
                                for (int j = 0; j < rowSize; ++j) {
                                    buffer[j] = ((BasicNanoTimestampVector) colOrigin).getLong(j) % 86400000000000L;
                                }
                                dstVector = new BasicNanoTimeVector(buffer);
                                break;
                            }
                            default:
                                throw new InterruptedException("Can't convert from " + dstType + " into " + getDTString(colOrigin.getDataType()));
                        }
                        break;
                    }
                    case "NANOTIMESTAMP": {
                        int[] buffer = new int[rowSize];
                        switch (colOrigin.getDataType()) {
                            case DT_NANOTIMESTAMP: {
                                dstVector = colOrigin;
                                break;
                            }
                            default:
                                throw new InterruptedException("Can't convert from " + dstType + " into " + getDTString(colOrigin.getDataType()));
                        }
                        break;
                    }
                    case "DATEHOUR": {
                        int[] buffer = new int[rowSize];
                        switch (colOrigin.getDataType()) {
                            case DT_DATETIME: {
                                for (int j = 0; j < rowSize; ++j) {
                                    buffer[j] = (int) (((BasicDateTimeVector) colOrigin).getInt(j) % 3600);
                                }
                                dstVector = new BasicDateHourVector(buffer);
                                break;
                            }
                            case DT_DATEHOUR: {
                                dstVector = colOrigin;
                                break;
                            }
                            case DT_TIMESTAMP: {
                                for (int j = 0; j < rowSize; ++j) {
                                    buffer[j] = (int) (((BasicTimestampVector) colOrigin).getLong(j) % 3600000);
                                }
                                dstVector = new BasicDateHourVector(buffer);
                                break;
                            }
                            case DT_NANOTIMESTAMP: {
                                for (int j = 0; j < rowSize; ++j) {
                                    buffer[j] = (int) (((BasicNanoTimestampVector) colOrigin).getLong(j) % 3600000000000L);
                                }
                                dstVector = new BasicDateHourVector(buffer);
                                break;
                            }
                            default:
                                throw new InterruptedException("Can't convert from " + dstType + " into " + getDTString(colOrigin.getDataType()));
                        }
                    }
                    default:dstVector=colOrigin;
                }
                cols.add(dstVector);
            }
            List<Entity> param = new ArrayList<Entity>();
            BasicTable paramTable = new BasicTable(colName, cols);
            param.add(paramTable);
            if(Objects.equals(dbUrl_, ""))
                res = con_.run("tableInsert{" + tableName_ + "}", param);
            else
                res = con_.run("tableInsert{loadTable(\"" + dbUrl_ + "\",\"" + tableName_ + "\"), }", param);
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }

        if (Objects.nonNull(res))
            log.info("AutoFitTableAppender.append() insert value rows: " + res.getString() + ".");

        return res;
    }

    public String getDTString(Entity.DATA_TYPE type) {
        switch (type) {
            case DT_ANY:
                return "ANY";
            case DT_BLOB:
                return "BLOB";
            case DT_BOOL:
                return "BOOL";
            case DT_BYTE:
                return "BYTE";
            case DT_CODE:
                return "CODE";
            case DT_COMPRESS:
                return "COMPRESSED";
            case DT_DATASOURCE:
                return "DATASOURCE";
            case DT_DATE:
                return "DATE";
            case DT_DATEHOUR:
                return "DATEHOUR";
            case DT_DATEMINUTE:
                return "DATEMINUTE";
            case DT_DATETIME:
                return "DATETIME";
            case DT_DICTIONARY:
                return "DICTIONARY";
            case DT_DOUBLE:
                return "DOUBLE";
            case DT_FLOAT:
                return "FLOAT";
            case DT_FUNCTIONDEF:
                return "FUNCTIONDEF";
            case DT_HANDLE:
                return "HANDLE";
            case DT_INT:
                return "INT";
            case DT_INT128:
                return "INT128";
            case DT_IPADDR:
                return "IPADDR";
            case DT_LONG:
                return "LONG";
            case DT_MINUTE:
                return "MINUTE";
            case DT_MONTH:
                return "MONTH";
            case DT_NANOTIME:
                return "NANOTIME";
            case DT_NANOTIMESTAMP:
                return "NANOTIMESTAMP";
            case DT_OBJECT:
                return "OBJECT";
            case DT_STRING:
                return "STRING";
            case DT_RESOURCE:
                return "RESOURCE";
            case DT_SECOND:
                return "SECOND";
            case DT_SHORT:
                return "SHORT";
            case DT_SYMBOL:
                return "SYMBOL";
            case DT_TIME:
                return "TIME";
            case DT_TIMESTAMP:
                return "TIMESTAMP";
            case DT_UUID:
                return "UUID";
            case DT_VOID:
                return "VOID";
        }

        return "Unrecognized type";
    }
}