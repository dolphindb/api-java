package com.xxdb.route;

import com.xxdb.DBConnection;
import com.xxdb.data.*;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

public class autoFitTableAppender{
    String dbUrl_;
    String tableName_;
    boolean async_;
    DBConnection con_;
    autoFitTableAppender(String dbUrl, String tableName, DBConnection con){
        this.dbUrl_=dbUrl;
        this.tableName_=tableName;
        this.con_=con;
    }
    public Entity append(List<Object> table){
        Entity ret;
        try {
            ret = con_.run("schema(loadTable(\"" + dbUrl_ + "\",\"" + tableName_ + "\")");
            int colSize = table.size();
            BasicTable schema = (BasicTable) ret;
            BasicStringVector typeList=(BasicStringVector)schema.getColumn("type");
            BasicStringVector nameList=(BasicStringVector)schema.getColumn("name");
            List<String> fileName=new ArrayList<>();
            List<Vector> col=new ArrayList<>();
            for (int i = 0; i < colSize; ++i) {
                String type=typeList.get(i).getString();
                fileName.add(nameList.getString(i));
                if(type == "DATE" || type == "MONTH" || type == "TIME" || type == "MINUTE" || type == "SECOND" || type == "DATETIME" || type == "DATE" || type == "DATEHOUR" || type == "TIMESTAMP" || type == "NANOTIME" || type == "NANOTIMESTAMP" || type == "DATEMINUTE") {
                    col.add((Vector) fillTypeVector((ArrayList<Object>) (table.get(i)), type));
                }
                else
                    col.add((Vector) table.get(i));
            }
            List<Entity> param=new ArrayList<Entity>();
            BasicTable paramTable=new BasicTable(fileName,col);
            ret = con_.run("append!{loadTable(\"" + dbUrl_ + "\",\"" + tableName_ + "\")}",param);

        } catch (IOException ioException) {
            ioException.printStackTrace();
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return new BasicBoolean(true);
    }
    public static Entity fillSchema(List<ArrayList> data, List<String> fieldName, List<String> typestr) throws java.lang.Exception{
        List<Vector> columns = new ArrayList<Vector>();
        List<String> names = fieldName;
        List<String> taggetTypeList=typestr;
        int size=data.size();
        for(int i=0;i<size;++i) {
            String clumnName = names.get(i);
            String targetTypeStr = taggetTypeList.get(i);
            Entity.DATA_TYPE targetType;
            switch (targetTypeStr) {
                case "DATE":
                    targetType = Entity.DATA_TYPE.DT_DATE;
                    break;
                case "MONTH":
                    targetType = Entity.DATA_TYPE.DT_MONTH;
                    break;
                case "TIME":
                    targetType = Entity.DATA_TYPE.DT_TIME;
                    break;
                case "MINUTE":
                    targetType = Entity.DATA_TYPE.DT_MINUTE;
                    break;
                case "SECOND":
                    targetType = Entity.DATA_TYPE.DT_SECOND;
                    break;
                case "DATETIME":
                    targetType = Entity.DATA_TYPE.DT_DATETIME;
                    break;
                case "DATEHOUR":
                    targetType = Entity.DATA_TYPE.DT_DATEHOUR;
                    break;
                case "TIMESTAMP":
                    targetType = Entity.DATA_TYPE.DT_TIMESTAMP;
                    break;
                case "NANOTIME":
                    targetType = Entity.DATA_TYPE.DT_NANOTIME;
                    break;
                case "NANOTIMESTAMP":
                    targetType = Entity.DATA_TYPE.DT_NANOTIMESTAMP;
                    break;
                case "DATEMINUTE":
                    targetType = Entity.DATA_TYPE.DT_DATETIME;
                    break;
                default:
                    throw new RuntimeException("The type " + targetTypeStr + " is not supported");
            }
            ArrayList<Object> dataClumn = data.get(i);
            int rowSize = dataClumn.size();
            String originClassString = dataClumn.get(0).getClass().toString();
            Vector tmpColmn;
            switch (targetType) {
                case DT_DATE: {
                    int[] buffer = new int[rowSize];

                    switch (originClassString) {
                        case "class java.time.LocalDateTime": {
                            for (int j = 0; j < rowSize; ++j) {
                                LocalDateTime timeTmp = (LocalDateTime) dataClumn.get(j);
                                buffer[j] = Utils.countDays(timeTmp.toLocalDate());
                            }
                            break;
                        }
                        case "class java.time.LocalDate": {
                            for (int j = 0; j < rowSize; ++j) {
                                LocalDate timeTmp = (LocalDate) dataClumn.get(j);
                                buffer[j] = Utils.countDays(timeTmp);
                            }
                            break;
                        }
                        default:
                            throw new IllegalArgumentException("Can't convert " + originClassString + " into DolphinDB " + targetTypeStr);
                    }
                    tmpColmn = new BasicDateVector(buffer);
                    break;
                }
                case DT_MONTH: {
                    int[] buffer = new int[rowSize];

                    switch (originClassString) {
                        case "class java.time.LocalDateTime": {
                            for (int j = 0; j < rowSize; ++j) {
                                LocalDateTime timeTmp = (LocalDateTime) dataClumn.get(j);
                                buffer[i] = timeTmp.getYear() * 12 + timeTmp.getMonth().getValue() - 1;
                            }
                            break;
                        }
                        case "class java.time.LocalDate": {
                            for (int j = 0; j < rowSize; ++j) {
                                LocalDate timeTmp = (LocalDate) dataClumn.get(j);
                                buffer[j] = timeTmp.getYear() + 12 + timeTmp.getMonth().getValue() - 1;
                            }
                            break;
                        }
                        default:
                            throw new IllegalArgumentException("Can't convert " + originClassString + " into DolphinDB " + targetTypeStr);
                    }
                    tmpColmn = new BasicDateVector(buffer);
                    break;
                }
                case DT_TIME: {
                    int[] buffer = new int[rowSize];

                    switch (originClassString) {
                        case "class java.time.LocalDateTime": {
                            for (int j = 0; j < rowSize; ++j) {
                                LocalDateTime timeTmp = (LocalDateTime) dataClumn.get(j);
                                buffer[j] = ((timeTmp.getHour() * 12 + timeTmp.getMinute()) * 60 + timeTmp.getSecond() * 1000) + timeTmp.getNano();
                            }
                            break;
                        }
                        case "class java.time.LocalTime": {
                            for (int j = 0; j < rowSize; ++j) {
                                LocalTime timeTmp = (LocalTime) dataClumn.get(j);
                                buffer[j] = ((timeTmp.getHour() * 12 + timeTmp.getMinute()) * 60 + timeTmp.getSecond() * 1000) + timeTmp.getNano();
                            }
                            break;
                        }
                        default:
                            throw new IllegalArgumentException("Can't convert " + originClassString + " into DolphinDB " + targetTypeStr);
                    }
                    tmpColmn = new BasicDateVector(buffer);
                    break;
                }
                case DT_MINUTE: {
                    int[] buffer = new int[rowSize];

                    switch (originClassString) {
                        case "class java.time.LocalDateTime": {
                            for (int j = 0; j < rowSize; ++j) {
                                LocalDateTime timeTmp = (LocalDateTime) dataClumn.get(j);
                                buffer[j] = timeTmp.getHour() * 12 + timeTmp.getMinute();
                            }
                            break;
                        }
                        case "class java.time.LocalTime": {
                            for (int j = 0; j < rowSize; ++j) {
                                LocalTime timeTmp = (LocalTime) dataClumn.get(j);
                                buffer[j] = timeTmp.getHour() * 12 + timeTmp.getMinute();
                            }
                            break;
                        }
                        default:
                            throw new IllegalArgumentException("Can't convert " + originClassString + " into DolphinDB " + targetTypeStr);
                    }
                    tmpColmn = new BasicDateVector(buffer);
                    break;
                }
                case DT_SECOND: {
                    int[] buffer = new int[rowSize];

                    switch (originClassString) {
                        case "class java.time.LocalDateTime": {
                            for (int j = 0; j < rowSize; ++j) {
                                LocalDateTime timeTmp = (LocalDateTime) dataClumn.get(j);
                                buffer[j] = (timeTmp.getHour() * 12 + timeTmp.getMinute()) * 60 + timeTmp.getSecond();
                            }
                            break;
                        }
                        case "class java.time.LocalTime": {
                            for (int j = 0; j < rowSize; ++j) {
                                LocalTime timeTmp = (LocalTime) dataClumn.get(j);
                                buffer[j] = (timeTmp.getHour() * 12 + timeTmp.getMinute()) * 60 + timeTmp.getSecond();
                            }
                            break;
                        }
                        default:
                            throw new IllegalArgumentException("Can't convert " + originClassString + " into DolphinDB " + targetTypeStr);
                    }
                    tmpColmn = new BasicDateVector(buffer);
                    break;
                }
                case DT_DATETIME: {
                    int[] buffer = new int[rowSize];

                    switch (originClassString) {
                        case "class java.time.LocalDateTime": {
                            for (int j = 0; j < rowSize; ++j) {
                                LocalDateTime timeTmp = (LocalDateTime) dataClumn.get(j);
                                buffer[j] = ((Utils.countDays(timeTmp.getYear(),timeTmp.getMonth().getValue(),timeTmp.getDayOfMonth())*24+timeTmp.getHour()) * 12 + timeTmp.getMinute()) * 60 + timeTmp.getSecond();
                            }
                            break;
                        }
                        default:
                            throw new IllegalArgumentException("Can't convert " + originClassString + " into DolphinDB " + targetTypeStr);
                    }
                    tmpColmn = new BasicDateVector(buffer);
                    break;
                }
                case DT_TIMESTAMP: {
                    int[] buffer = new int[rowSize];

                    switch (originClassString) {
                        default:
                            throw new IllegalArgumentException("Can't convert " + originClassString + " into DolphinDB " + targetTypeStr);
                    }
                    //tmpColmn = new BasicDateVector(buffer);
                    //break;
                }
                case DT_NANOTIME: {
                    int[] buffer = new int[rowSize];

                    switch (originClassString) {
                        case "":break;
                        default:
                            throw new IllegalArgumentException("Can't convert " + originClassString + " into DolphinDB " + targetTypeStr);
                    }
                    tmpColmn = new BasicDateVector(buffer);
                    break;
                }
                case DT_NANOTIMESTAMP: {
                    int[] buffer = new int[rowSize];

                    switch (originClassString) {
                        default:
                            throw new IllegalArgumentException("Can't convert from "+originClassString + " into " + targetTypeStr+" in DolphinDB");
                    }
//					tmpColmn = new BasicDateVector(buffer);
//					break;
                }
                default:
                    throw new IllegalArgumentException("Unsupported DolphinDB type");
            }
            columns.add(tmpColmn);
        }
        BasicTable ret=new BasicTable(names, columns);
        return ret;
    }

    public static Entity fillTypeVector(ArrayList<Object> data, String typestr) throws java.lang.Exception{
        List<Vector> columns = new ArrayList<Vector>();
        int size=data.size();
        AbstractVector ret;
        String targetTypeStr = typestr;
        Entity.DATA_TYPE targetType;
        switch (targetTypeStr) {
            case "DATE":
                targetType = Entity.DATA_TYPE.DT_DATE;
                break;
            case "MONTH":
                targetType = Entity.DATA_TYPE.DT_MONTH;
                break;
            case "TIME":
                targetType = Entity.DATA_TYPE.DT_TIME;
                break;
            case "MINUTE":
                targetType = Entity.DATA_TYPE.DT_MINUTE;
                break;
            case "SECOND":
                targetType = Entity.DATA_TYPE.DT_SECOND;
                break;
            case "DATETIME":
                targetType = Entity.DATA_TYPE.DT_DATETIME;
                break;
            case "DATEHOUR":
                targetType = Entity.DATA_TYPE.DT_DATEHOUR;
                break;
            case "TIMESTAMP":
                targetType = Entity.DATA_TYPE.DT_TIMESTAMP;
                break;
            case "NANOTIME":
                targetType = Entity.DATA_TYPE.DT_NANOTIME;
                break;
            case "NANOTIMESTAMP":
                targetType = Entity.DATA_TYPE.DT_NANOTIMESTAMP;
                break;
            case "DATEMINUTE":
                targetType = Entity.DATA_TYPE.DT_DATETIME;
                break;
            default:
                throw new RuntimeException("The type " + targetTypeStr + " is not supported");
        }
        int rowSize = data.size();
        String originClassString = data.get(0).getClass().toString();
        switch (targetType) {
            case DT_DATE: {
                int[] buffer = new int[rowSize];

                switch (originClassString) {
                    case "class java.time.LocalDateTime": {
                        for (int j = 0; j < rowSize; ++j) {
                            LocalDateTime timeTmp = (LocalDateTime) data.get(j);
                            buffer[j] = Utils.countDays(timeTmp.toLocalDate());
                        }
                        break;
                    }
                    case "class java.time.LocalDate": {
                        for (int j = 0; j < rowSize; ++j) {
                            LocalDate timeTmp = (LocalDate) data.get(j);
                            buffer[j] = Utils.countDays(timeTmp);
                        }
                        break;
                    }
                    default:
                        throw new IllegalArgumentException("Can't convert " + originClassString + " into DolphinDB " + targetTypeStr);
                }
                ret = new BasicDateVector(buffer);
                break;
            }
            case DT_MONTH: {
                int[] buffer = new int[rowSize];

                switch (originClassString) {
                    case "class java.time.LocalDateTime": {
                        for (int j = 0; j < rowSize; ++j) {
                            LocalDateTime timeTmp = (LocalDateTime) data.get(j);
                            buffer[j] = timeTmp.getYear() * 12 + timeTmp.getMonth().getValue() - 1;
                        }
                        break;
                    }
                    case "class java.time.LocalDate": {
                        for (int j = 0; j < rowSize; ++j) {
                            LocalDate timeTmp = (LocalDate) data.get(j);
                            buffer[j] = timeTmp.getYear() + 12 + timeTmp.getMonth().getValue() - 1;
                        }
                        break;
                    }
                    default:
                        throw new IllegalArgumentException("Can't convert " + originClassString + " into DolphinDB " + targetTypeStr);
                }
                ret = new BasicDateVector(buffer);
                break;
            }
            case DT_TIME: {
                int[] buffer = new int[rowSize];

                switch (originClassString) {
                    case "class java.time.LocalDateTime": {
                        for (int j = 0; j < rowSize; ++j) {
                            LocalDateTime timeTmp = (LocalDateTime) data.get(j);
                            buffer[j] = ((timeTmp.getHour() * 12 + timeTmp.getMinute()) * 60 + timeTmp.getSecond() * 1000) + timeTmp.getNano();
                        }
                        break;
                    }
                    case "class java.time.LocalTime": {
                        for (int j = 0; j < rowSize; ++j) {
                            LocalTime timeTmp = (LocalTime) data.get(j);
                            buffer[j] = ((timeTmp.getHour() * 12 + timeTmp.getMinute()) * 60 + timeTmp.getSecond() * 1000) + timeTmp.getNano();
                        }
                        break;
                    }
                    default:
                        throw new IllegalArgumentException("Can't convert " + originClassString + " into DolphinDB " + targetTypeStr);
                }
                ret = new BasicDateVector(buffer);
                break;
            }
            case DT_MINUTE: {
                int[] buffer = new int[rowSize];

                switch (originClassString) {
                    case "class java.time.LocalDateTime": {
                        for (int j = 0; j < rowSize; ++j) {
                            LocalDateTime timeTmp = (LocalDateTime) data.get(j);
                            buffer[j] = timeTmp.getHour() * 12 + timeTmp.getMinute();
                        }
                        break;
                    }
                    case "class java.time.LocalTime": {
                        for (int j = 0; j < rowSize; ++j) {
                            LocalTime timeTmp = (LocalTime) data.get(j);
                            buffer[j] = timeTmp.getHour() * 12 + timeTmp.getMinute();
                        }
                        break;
                    }
                    default:
                        throw new IllegalArgumentException("Can't convert " + originClassString + " into DolphinDB " + targetTypeStr);
                }
                ret = new BasicDateVector(buffer);
                break;
            }
            case DT_SECOND: {
                int[] buffer = new int[rowSize];

                switch (originClassString) {
                    case "class java.time.LocalDateTime": {
                        for (int j = 0; j < rowSize; ++j) {
                            LocalDateTime timeTmp = (LocalDateTime) data.get(j);
                            buffer[j] = (timeTmp.getHour() * 12 + timeTmp.getMinute()) * 60 + timeTmp.getSecond();
                        }
                        break;
                    }
                    case "class java.time.LocalTime": {
                        for (int j = 0; j < rowSize; ++j) {
                            LocalTime timeTmp = (LocalTime) data.get(j);
                            buffer[j] = (timeTmp.getHour() * 12 + timeTmp.getMinute()) * 60 + timeTmp.getSecond();
                        }
                        break;
                    }
                    default:
                        throw new IllegalArgumentException("Can't convert " + originClassString + " into DolphinDB " + targetTypeStr);
                }
                ret = new BasicDateVector(buffer);
                break;
            }
            case DT_DATETIME: {
                int[] buffer = new int[rowSize];

                switch (originClassString) {
                    case "class java.time.LocalDateTime": {
                        for (int j = 0; j < rowSize; ++j) {
                            LocalDateTime timeTmp = (LocalDateTime) data.get(j);
                            buffer[j] = ((Utils.countDays(timeTmp.getYear(),timeTmp.getMonth().getValue(),timeTmp.getDayOfMonth())*24+timeTmp.getHour()) * 12 + timeTmp.getMinute()) * 60 + timeTmp.getSecond();
                        }
                        break;
                    }
                    default:
                        throw new IllegalArgumentException("Can't convert " + originClassString + " into DolphinDB " + targetTypeStr);
                }
                ret = new BasicDateVector(buffer);
                break;
            }
            case DT_TIMESTAMP: {
                int[] buffer = new int[rowSize];

                switch (originClassString) {
                    default:
                        throw new IllegalArgumentException("Can't convert " + originClassString + " into DolphinDB " + targetTypeStr);
                }
                //tmpColmn = new BasicDateVector(buffer);
                //break;
            }
            case DT_NANOTIME: {
                int[] buffer = new int[rowSize];

                switch (originClassString) {
                    case "":break;
                    default:
                        throw new IllegalArgumentException("Can't convert " + originClassString + " into DolphinDB " + targetTypeStr);
                }
                ret = new BasicDateVector(buffer);
                break;
            }
            case DT_NANOTIMESTAMP: {
                int[] buffer = new int[rowSize];

                switch (originClassString) {
                    default:
                        throw new IllegalArgumentException("Can't convert from "+originClassString + " into " + targetTypeStr+" in DolphinDB");
                }
//					tmpColmn = new BasicDateVector(buffer);
//					break;
            }
            default:
                throw new IllegalArgumentException("Unsupported DolphinDB type");
        }
        return ret;
    }
}