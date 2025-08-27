package com.xxdb.streaming.client.streamingSQL;

import com.xxdb.data.*;
import com.xxdb.data.Vector;
import com.xxdb.streaming.client.IMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

public class StreamingSQLResultUpdater {

    private static class LogType {
        public static final byte kUpdate = 0;
        public static final byte kAppend = 1;
        public static final byte kDelete = 2;
        public static final byte kInsert = 3;
    }

    private static final Logger log = LoggerFactory.getLogger(StreamingSQLResultUpdater.class);

    /**
     * Used for sharing modifiable arrays within methods
     */
    private static class DeleteLineMapWrapper {
        public BasicIntVector map;

        public DeleteLineMapWrapper(BasicIntVector map) {
            this.map = map;
        }
    }

    /**
     * Streaming SQL result wrapper class, containing table and row number mapping
     */
    public static class StreamingSQLResult {
        public BasicTable table;
        public BasicIntVector deleteLineMap;

        public StreamingSQLResult(BasicTable table, BasicIntVector deleteLineMap) {
            this.table = table;
            this.deleteLineMap = deleteLineMap;
        }
    }

    /**
     * Update streaming SQL result table
     * @param result The result table to update
     * @param deleteLineMap Row number mapping
     * @param msg Log message
     * @return Result object containing updated table and row number mapping
     * @throws Exception If an error occurs during update
     */
    public static StreamingSQLResult updateStreamingSQLResult(BasicTable result, BasicIntVector deleteLineMap, IMessage msg) throws Exception {
        log.debug("Starting updateStreamingSQLResult");
        log.debug("Table columns: " + result.columns() + ", rows: " + result.rows());

        BasicByteVector typeColumn = new BasicByteVector(0);
        typeColumn.Append((BasicByte) msg.getEntity(0));

        byte msgType = ((BasicByte)msg.getEntity(0)).getByte();
        log.debug("Message type: " + msgType);

        BasicIntVector lineNoColumn = new BasicIntVector(0);
        lineNoColumn.Append((BasicInt) msg.getEntity(1));
        List<Vector> updateColumns = new ArrayList<>();
        for (int i = 2; i < msg.size(); i++) {
            if (msg.getEntity(i) instanceof Vector) {
                updateColumns.add((Vector) msg.getEntity(i));
            } else {
                Vector col;
                if (msg.getEntity(i) instanceof BasicDecimal32 || msg.getEntity(i) instanceof BasicDecimal64 || msg.getEntity(i) instanceof BasicDecimal128) {
                    col =  BasicEntityFactory.instance().createVectorWithDefaultValue(msg.getEntity(i).getDataType(), 0, ((Scalar) msg.getEntity(i)).getScale());
                } else {
                    col = BasicEntityFactory.instance().createVectorWithDefaultValue(msg.getEntity(i).getDataType(), 0, -1);
                }

                col.Append((Scalar) msg.getEntity(i));
                updateColumns.add(col);
            }
        }

        int updateLogSize = lineNoColumn.rows();
        final Vector[] updateValues = new Vector[updateColumns.size()];

        // Create wrapper object for deleteLineMap
        DeleteLineMapWrapper wrapper = new DeleteLineMapWrapper(deleteLineMap);

        String err = "";
        int start = 0;
        int offset = 0;
        int length = 0;

        byte prevUpdateType = -1;
        if (lineNoColumn.rows() == 1) {
            // If lineNoColumn is a scalar, convert it to a vector
            BasicIntVector vec = new BasicIntVector(1);
            vec.set(0, lineNoColumn.get(0));
            lineNoColumn = vec;
        }

        // Process update log
        while (start < updateLogSize) {
            int len = Math.min(updateLogSize - start, 1024);
            byte[] ptype = new byte[len];
            for (int i = 0; i < len; i++) {
                ptype[i] = ((BasicByte)typeColumn.get(start + i)).getByte();
                log.debug("Type at index " + (start + i) + ": " + ptype[i]);
            }

            for (int i = 0; i < len; i++) {
                if (ptype[i] != prevUpdateType) {
                    if (length > 0) {
                        // Process previous operation group
                        log.debug("Processing operation type: " + prevUpdateType);
                        synchronized (result) {
                            if (prevUpdateType == LogType.kUpdate) {
                                // Process update operation
                                getLinesInLog(updateColumns, updateValues, updateLogSize, offset, length);

                                // If table is empty, treat as append operation
                                if (result.rows() == 0) {
                                    log.debug("Table is empty, treating kUpdate as kAppend");
                                    boolean appended = appendColumns(result, updateValues);
                                    if (!appended) {
                                        throw new RuntimeException("updateStreamingSQLResult failed with error: " + err);
                                    }
                                } else {
                                    List<String> columnNames = new ArrayList<>();
                                    for (int j = 0; j < result.columns(); ++j) {
                                        columnNames.add(result.getColumnName(j));
                                    }

                                    // Get updated row indices
                                    BasicIntVector updateLineNo = (BasicIntVector)((AbstractVector)lineNoColumn).getSubVector(createRangeIndices(offset, length));

                                    // Separate out-of-range indices and valid indices
                                    List<Integer> validIndices = new ArrayList<>();
                                    List<Integer> outOfRangeIndices = new ArrayList<>();

                                    for (int idx = 0; idx < updateLineNo.rows(); idx++) {
                                        int lineNo = ((BasicInt)updateLineNo.get(idx)).getInt();
                                        if (lineNo < result.rows()) {
                                            validIndices.add(idx);
                                        } else {
                                            outOfRangeIndices.add(idx);
                                        }
                                    }

                                    // Process valid update indices
                                    if (!validIndices.isEmpty()) {
                                        // Create update vectors containing only valid indices
                                        BasicIntVector validUpdateLineNo = new BasicIntVector(validIndices.size());
                                        Vector[] validUpdateValues = new Vector[updateValues.length];

                                        for (int colIdx = 0; colIdx < updateValues.length; colIdx++) {
                                            int[] validIndicesArray = new int[validIndices.size()];
                                            for (int j = 0; j < validIndices.size(); j++) {
                                                validIndicesArray[j] = validIndices.get(j);
                                                if (colIdx == 0) { // Only set row index for the first column
                                                    validUpdateLineNo.set(j, updateLineNo.get(validIndices.get(j)));
                                                }
                                            }
                                            validUpdateValues[colIdx] = ((AbstractVector)updateValues[colIdx]).getSubVector(validIndicesArray);
                                        }

                                        updateIndexMapping(validUpdateLineNo, result.rows(), wrapper);
                                        boolean updated = updateRows(result, validUpdateValues, validUpdateLineNo, columnNames);
                                        if (!updated) {
                                            throw new RuntimeException("updateStreamingSQLResult failed with error: " + err);
                                        }
                                    }

                                    // Process out-of-range indices as append operations
                                    if (!outOfRangeIndices.isEmpty()) {
                                        log.debug("Found " + outOfRangeIndices.size() + " indices out of range, treating as append operations");

                                        // Create append vectors containing only out-of-range indices
                                        Vector[] appendValues = new Vector[updateValues.length];

                                        for (int colIdx = 0; colIdx < updateValues.length; colIdx++) {
                                            int[] outOfRangeIndicesArray = new int[outOfRangeIndices.size()];
                                            for (int j = 0; j < outOfRangeIndices.size(); j++) {
                                                outOfRangeIndicesArray[j] = outOfRangeIndices.get(j);
                                            }
                                            appendValues[colIdx] = ((AbstractVector)updateValues[colIdx]).getSubVector(outOfRangeIndicesArray);
                                        }

                                        boolean appended = appendColumns(result, appendValues);
                                        if (!appended) {
                                            throw new RuntimeException("updateStreamingSQLResult failed with error: " + err);
                                        }
                                    }
                                }
                            } else if (prevUpdateType == LogType.kAppend) {
                                // Process append operation
                                getLinesInLog(updateColumns, updateValues, updateLogSize, offset, length);
                                log.debug("Calling appendColumns for kAppend");
                                boolean appended = appendColumns(result, updateValues);
                                if (!appended) {
                                    throw new RuntimeException("updateStreamingSQLResult failed with error: " + err);
                                }
                            } else if (prevUpdateType == LogType.kDelete) {
                                // Process delete operation
                                BasicIntVector removeLineNo = (BasicIntVector)((AbstractVector)lineNoColumn).getSubVector(createRangeIndices(offset, length));
                                removeIndexMapping(removeLineNo, result.rows(), wrapper);
                                // 先排序再删除
                                int[] originalArray = removeLineNo.getdataArray();
                                int[] sortedArray = originalArray.clone();
                                Arrays.sort(sortedArray);
                                removeLineNo = new BasicIntVector(sortedArray);

                                if (result.rows() > 0) {
                                    result = removeRows(result, removeLineNo);
                                }
                            } else if (prevUpdateType == LogType.kInsert) {
                                // Process insert operation
                                log.debug("处理 kInsert 类型消息");

                                // 获取插入行号
                                BasicIntVector insertNo = (BasicIntVector)((AbstractVector)lineNoColumn).getSubVector(createRangeIndices(offset, length));
                                int prevSize = result.rows();

                                // 打印消息内容
                                log.debug("消息内容: 类型=" + prevUpdateType + ", 行号=" + insertNo.getString() +
                                        ", 数据列数=" + (msg.size() - 2));

                                // 如果是单行数据，使用通用类型处理
                                if (insertNo.rows() == 1) {
                                    // 创建包含新数据的列数组
                                    Vector[] newColumns = new Vector[msg.size() - 2]; // 减去类型和行号两列

                                    // 为每一列创建对应类型的向量
                                    for (int j = 2; j < msg.size(); j++) {
                                        Scalar sourceValue = (Scalar) msg.getEntity(j);
                                        Entity.DATA_TYPE dataType = sourceValue.getDataType();

                                        // 创建适当类型的向量
                                        Vector newVector;
                                        if (msg.getEntity(j) instanceof BasicDecimal32 || msg.getEntity(j) instanceof BasicDecimal64 || msg.getEntity(j) instanceof BasicDecimal128) {
                                            newVector = BasicEntityFactory.instance().createVectorWithDefaultValue(dataType, 1, ((Scalar) msg.getEntity(j)).getScale());
                                        } else {
                                            newVector = BasicEntityFactory.instance().createVectorWithDefaultValue(dataType, 1, -1);
                                        }

                                        // 根据数据类型正确复制值
                                        switch (dataType) {
                                            case DT_BOOL:
                                                boolean boolValue = ((BasicBoolean)sourceValue).getBoolean();
                                                ((BasicBooleanVector)newVector).set(0, new BasicBoolean(boolValue));
                                                break;
                                            case DT_BYTE:
                                                byte byteValue = ((BasicByte)sourceValue).getByte();
                                                ((BasicByteVector)newVector).set(0, new BasicByte(byteValue));
                                                break;
                                            case DT_SHORT:
                                                short shortValue = ((BasicShort)sourceValue).getShort();
                                                ((BasicShortVector)newVector).set(0, new BasicShort(shortValue));
                                                break;
                                            case DT_INT:
                                                int intValue = ((BasicInt)sourceValue).getInt();
                                                ((BasicIntVector)newVector).set(0, new BasicInt(intValue));
                                                break;
                                            case DT_LONG:
                                                long longValue = ((BasicLong)sourceValue).getLong();
                                                ((BasicLongVector)newVector).set(0, new BasicLong(longValue));
                                                break;
                                            case DT_FLOAT:
                                                float floatValue = ((BasicFloat)sourceValue).getFloat();
                                                ((BasicFloatVector)newVector).set(0, new BasicFloat(floatValue));
                                                break;
                                            case DT_DOUBLE:
                                                double doubleValue = ((BasicDouble)sourceValue).getDouble();
                                                ((BasicDoubleVector)newVector).set(0, new BasicDouble(doubleValue));
                                                break;
                                            case DT_STRING:
                                                String stringValue = ((BasicString)sourceValue).getString();
                                                ((BasicStringVector)newVector).set(0, new BasicString(stringValue));
                                                break;
                                            case DT_DATE:
                                                int dateValue = ((BasicDate)sourceValue).getInt();
                                                ((BasicDateVector)newVector).set(0, new BasicDate(dateValue));
                                                break;
                                            case DT_MONTH:
                                                int monthValue = ((BasicMonth)sourceValue).getInt();
                                                ((BasicMonthVector)newVector).set(0, new BasicMonth(monthValue));
                                                break;
                                            case DT_TIME:
                                                int timeValue = ((BasicTime)sourceValue).getInt();
                                                ((BasicTimeVector)newVector).set(0, new BasicTime(timeValue));
                                                break;
                                            case DT_MINUTE:
                                                int minuteValue = ((BasicMinute)sourceValue).getInt();
                                                ((BasicMinuteVector)newVector).set(0, new BasicMinute(minuteValue));
                                                break;
                                            case DT_SECOND:
                                                int secondValue = ((BasicSecond)sourceValue).getInt();
                                                ((BasicSecondVector)newVector).set(0, new BasicSecond(secondValue));
                                                break;
                                            case DT_DATETIME:
                                                int datetimeValue = ((BasicDateTime)sourceValue).getInt();
                                                ((BasicDateTimeVector)newVector).set(0, new BasicDateTime(datetimeValue));
                                                break;
                                            case DT_TIMESTAMP:
                                                long timestampValue = ((BasicTimestamp)sourceValue).getLong();
                                                ((BasicTimestampVector)newVector).set(0, new BasicTimestamp(timestampValue));
                                                break;
                                            case DT_NANOTIME:
                                                long nanotimeValue = ((BasicNanoTime)sourceValue).getLong();
                                                ((BasicNanoTimeVector)newVector).set(0, new BasicNanoTime(nanotimeValue));
                                                break;
                                            case DT_NANOTIMESTAMP:
                                                long nanotimestampValue = ((BasicNanoTimestamp)sourceValue).getLong();
                                                ((BasicNanoTimestampVector)newVector).set(0, new BasicNanoTimestamp(nanotimestampValue));
                                                break;
                                            // 可以根据需要添加更多类型
                                            default:
                                                // 对于不认识的类型，直接复制原始值
                                                newVector.set(0, sourceValue);
                                        }

                                        newColumns[j-2] = newVector;
                                    }

                                    // 直接添加到表中
                                    boolean appended = appendColumns(result, newColumns);
                                    if (!appended) {
                                        throw new RuntimeException("updateStreamingSQLResult failed when appending new data");
                                    }

                                    log.debug("成功添加新行，当前表行数: " + result.rows());

                                    // 获取逻辑插入位置
                                    int logicalInsertPos = ((BasicInt)insertNo.get(0)).getInt();
                                    log.debug("逻辑插入位置: " + logicalInsertPos);

                                    // 计算物理插入位置（考虑deleteLineMap）
                                    int physicalInsertPos = logicalInsertPos;
                                    int lineMapSize = wrapper.map != null ? wrapper.map.rows() : 0;
                                    int[] mapArray = wrapper.map != null ? wrapper.map.getdataArray() : new int[0];

                                    if (lineMapSize > 0) {
                                        // 找到在deleteLineMap中小于logicalInsertPos的元素个数
                                        int mapPos = findLowerBoundCount(mapArray, lineMapSize, logicalInsertPos);
                                        physicalInsertPos = logicalInsertPos - mapPos;

                                        // 更新deleteLineMap：在插入位置之后的所有删除记录都需要+1
                                        for (int j = mapPos; j < lineMapSize; j++) {
                                            mapArray[j]++;
                                        }
                                        wrapper.map = new BasicIntVector(mapArray);

                                        log.debug("考虑deleteLineMap后，物理插入位置: " + physicalInsertPos +
                                                ", mapPos: " + mapPos);
                                    }

                                    // 边界检查
                                    if (physicalInsertPos > prevSize) {
                                        physicalInsertPos = prevSize;
                                    } else if (physicalInsertPos < 0) {
                                        physicalInsertPos = 0;
                                    }

                                    log.debug("最终物理插入位置: " + physicalInsertPos);

                                    // 只有当插入位置不是在末尾时，才需要重排序
                                    if (physicalInsertPos < prevSize) {
                                        // 创建一个表示当前表行顺序的向量
                                        BasicIntVector sortIndex = new BasicIntVector(result.rows());
                                        int[] sortArray = sortIndex.getdataArray();

                                        // 初始化排序数组
                                        for (int j = 0; j < result.rows(); j++) {
                                            sortArray[j] = j;
                                        }

                                        // 将最后一行（新插入的行）移动到指定位置
                                        int lastRowIndex = prevSize; // 新插入行的索引

                                        // 向右移动元素，为新行腾出空间
                                        for (int j = lastRowIndex; j > physicalInsertPos; j--) {
                                            sortArray[j] = sortArray[j - 1];
                                        }
                                        sortArray[physicalInsertPos] = lastRowIndex;

                                        // 使用排序索引创建新表
                                        List<Vector> newCols = new ArrayList<>();
                                        for (int col = 0; col < result.columns(); col++) {
                                            newCols.add(result.getColumn(col).getSubVector(sortArray));
                                        }
                                        result = new BasicTable(getColumnNames(result), newCols);

                                        log.debug("完成单行插入重排序");
                                    }
                                } else {
                                    // 对于多行数据的处理
                                    getLinesInLog(updateColumns, updateValues, updateLogSize, offset, length);

                                    // 添加到表中
                                    boolean appended = appendColumns(result, updateValues);
                                    if (!appended) {
                                        throw new RuntimeException("updateStreamingSQLResult failed with error: " + err);
                                    }

                                    log.debug("成功添加新行，当前表行数: " + result.rows());

                                    // 计算排序索引
                                    BasicIntVector sortIndex = insertIndexMapping(prevSize, insertNo, wrapper);

                                    // 使用排序索引创建新表
                                    List<Vector> newCols = new ArrayList<>();
                                    for (int col = 0; col < result.columns(); col++) {
                                        newCols.add(result.getColumn(col).getSubVector(sortIndex.getdataArray()));
                                    }

                                    // 创建并替换为新表 - 确保列顺序正确
                                    List<String> columnNames = getColumnNames(result);
                                    result = new BasicTable(columnNames, newCols);

                                    log.debug("重排序后，表有 " + result.rows() + " 行");
                                }

                                log.debug("处理完成，当前表行数: " + result.rows());
                            }
                        }
                    }
                    offset = start + i;
                    length = 1;
                    prevUpdateType = ptype[i];
                } else {
                    length++;
                }
            }
            start += len;
        }

        // Process the last operation group
        if (length > 0) {
            // Process previous operation group
            log.debug("Processing final operation type: " + prevUpdateType);
            synchronized (result) {
                if (prevUpdateType == LogType.kUpdate) {
                    // Process update operation
                    getLinesInLog(updateColumns, updateValues, updateLogSize, offset, length);

                    // If table is empty, treat as append operation
                    if (result.rows() == 0) {
                        log.debug("Table is empty, treating final kUpdate as kAppend");
                        boolean appended = appendColumns(result, updateValues);
                        if (!appended) {
                            throw new RuntimeException("updateStreamingSQLResult failed with error: " + err);
                        }
                    } else {
                        List<String> columnNames = new ArrayList<>();
                        for (int j = 0; j < result.columns(); ++j) {
                            columnNames.add(result.getColumnName(j));
                        }

                        // Get updated row indices
                        BasicIntVector updateLineNo = (BasicIntVector)((AbstractVector)lineNoColumn).getSubVector(createRangeIndices(offset, length));

                        // Separate out-of-range indices and valid indices
                        List<Integer> validIndices = new ArrayList<>();
                        List<Integer> outOfRangeIndices = new ArrayList<>();

                        for (int idx = 0; idx < updateLineNo.rows(); idx++) {
                            int lineNo = ((BasicInt)updateLineNo.get(idx)).getInt();
                            if (lineNo < result.rows()) {
                                validIndices.add(idx);
                            } else {
                                outOfRangeIndices.add(idx);
                            }
                        }

                        // Process valid update indices
                        if (!validIndices.isEmpty()) {
                            // Create update vectors containing only valid indices
                            BasicIntVector validUpdateLineNo = new BasicIntVector(validIndices.size());
                            Vector[] validUpdateValues = new Vector[updateValues.length];

                            for (int colIdx = 0; colIdx < updateValues.length; colIdx++) {
                                int[] validIndicesArray = new int[validIndices.size()];
                                for (int j = 0; j < validIndices.size(); j++) {
                                    validIndicesArray[j] = validIndices.get(j);
                                    if (colIdx == 0) { // Only set row index for the first column
                                        validUpdateLineNo.set(j, updateLineNo.get(validIndices.get(j)));
                                    }
                                }
                                validUpdateValues[colIdx] = ((AbstractVector)updateValues[colIdx]).getSubVector(validIndicesArray);
                            }

                            updateIndexMapping(validUpdateLineNo, result.rows(), wrapper);
                            boolean updated = updateRows(result, validUpdateValues, validUpdateLineNo, columnNames);
                            if (!updated) {
                                throw new RuntimeException("updateStreamingSQLResult failed with error: " + err);
                            }
                        }

                        // Process out-of-range indices as append operations
                        if (!outOfRangeIndices.isEmpty()) {
                            log.debug("Found " + outOfRangeIndices.size() + " indices out of range, treating as append operations");

                            // Create append vectors containing only out-of-range indices
                            Vector[] appendValues = new Vector[updateValues.length];

                            for (int colIdx = 0; colIdx < updateValues.length; colIdx++) {
                                int[] outOfRangeIndicesArray = new int[outOfRangeIndices.size()];
                                for (int j = 0; j < outOfRangeIndices.size(); j++) {
                                    outOfRangeIndicesArray[j] = outOfRangeIndices.get(j);
                                }
                                appendValues[colIdx] = ((AbstractVector)updateValues[colIdx]).getSubVector(outOfRangeIndicesArray);
                            }

                            boolean appended = appendColumns(result, appendValues);
                            if (!appended) {
                                throw new RuntimeException("updateStreamingSQLResult failed with error: " + err);
                            }
                        }
                    }
                } else if (prevUpdateType == LogType.kAppend) {
                    // Process append operation
                    getLinesInLog(updateColumns, updateValues, updateLogSize, offset, length);
                    log.debug("Calling appendColumns for final kAppend");
                    boolean appended = appendColumns(result, updateValues);
                    if (!appended) {
                        throw new RuntimeException("updateStreamingSQLResult failed with error: " + err);
                    }
                } else if (prevUpdateType == LogType.kDelete) {
                    // Process delete operation
                    BasicIntVector removeLineNo = (BasicIntVector)((AbstractVector)lineNoColumn).getSubVector(createRangeIndices(offset, length));
                    removeIndexMapping(removeLineNo, result.rows(), wrapper);
                    // 先排序再删除
                    int[] originalArray = removeLineNo.getdataArray();
                    int[] sortedArray = originalArray.clone();
                    Arrays.sort(sortedArray);
                    removeLineNo = new BasicIntVector(sortedArray);

                    if (result.rows() > 0) {
                        result = removeRows(result, removeLineNo);
                    }
                } else if (prevUpdateType == LogType.kInsert) {
                    // Process insert operation
                    log.debug("处理 kInsert 类型消息");

                    // 获取插入行号
                    BasicIntVector insertNo = (BasicIntVector)((AbstractVector)lineNoColumn).getSubVector(createRangeIndices(offset, length));
                    int prevSize = result.rows();

                    // 打印消息内容
                    log.debug("消息内容: 类型=" + prevUpdateType + ", 行号=" + insertNo.getString() +
                            ", 数据列数=" + (msg.size() - 2));

                    // 如果是单行数据，使用通用类型处理
                    if (insertNo.rows() == 1) {
                        // 创建包含新数据的列数组
                        Vector[] newColumns = new Vector[msg.size() - 2]; // 减去类型和行号两列

                        // 为每一列创建对应类型的向量
                        for (int j = 2; j < msg.size(); j++) {
                            Scalar sourceValue = (Scalar) msg.getEntity(j);
                            Entity.DATA_TYPE dataType = sourceValue.getDataType();

                            // 创建适当类型的向量
                            Vector newVector;
                            if (msg.getEntity(j) instanceof BasicDecimal32 || msg.getEntity(j) instanceof BasicDecimal64 || msg.getEntity(j) instanceof BasicDecimal128) {
                                newVector = BasicEntityFactory.instance().createVectorWithDefaultValue(dataType, 1, ((Scalar) msg.getEntity(j)).getScale());
                            } else {
                                newVector = BasicEntityFactory.instance().createVectorWithDefaultValue(dataType, 1, -1);
                            }

                            // 根据数据类型正确复制值
                            switch (dataType) {
                                case DT_BOOL:
                                    boolean boolValue = ((BasicBoolean)sourceValue).getBoolean();
                                    ((BasicBooleanVector)newVector).set(0, new BasicBoolean(boolValue));
                                    break;
                                case DT_BYTE:
                                    byte byteValue = ((BasicByte)sourceValue).getByte();
                                    ((BasicByteVector)newVector).set(0, new BasicByte(byteValue));
                                    break;
                                case DT_SHORT:
                                    short shortValue = ((BasicShort)sourceValue).getShort();
                                    ((BasicShortVector)newVector).set(0, new BasicShort(shortValue));
                                    break;
                                case DT_INT:
                                    int intValue = ((BasicInt)sourceValue).getInt();
                                    ((BasicIntVector)newVector).set(0, new BasicInt(intValue));
                                    break;
                                case DT_LONG:
                                    long longValue = ((BasicLong)sourceValue).getLong();
                                    ((BasicLongVector)newVector).set(0, new BasicLong(longValue));
                                    break;
                                case DT_FLOAT:
                                    float floatValue = ((BasicFloat)sourceValue).getFloat();
                                    ((BasicFloatVector)newVector).set(0, new BasicFloat(floatValue));
                                    break;
                                case DT_DOUBLE:
                                    double doubleValue = ((BasicDouble)sourceValue).getDouble();
                                    ((BasicDoubleVector)newVector).set(0, new BasicDouble(doubleValue));
                                    break;
                                case DT_STRING:
                                    String stringValue = ((BasicString)sourceValue).getString();
                                    ((BasicStringVector)newVector).set(0, new BasicString(stringValue));
                                    break;
                                case DT_DATE:
                                    int dateValue = ((BasicDate)sourceValue).getInt();
                                    ((BasicDateVector)newVector).set(0, new BasicDate(dateValue));
                                    break;
                                case DT_MONTH:
                                    int monthValue = ((BasicMonth)sourceValue).getInt();
                                    ((BasicMonthVector)newVector).set(0, new BasicMonth(monthValue));
                                    break;
                                case DT_TIME:
                                    int timeValue = ((BasicTime)sourceValue).getInt();
                                    ((BasicTimeVector)newVector).set(0, new BasicTime(timeValue));
                                    break;
                                case DT_MINUTE:
                                    int minuteValue = ((BasicMinute)sourceValue).getInt();
                                    ((BasicMinuteVector)newVector).set(0, new BasicMinute(minuteValue));
                                    break;
                                case DT_SECOND:
                                    int secondValue = ((BasicSecond)sourceValue).getInt();
                                    ((BasicSecondVector)newVector).set(0, new BasicSecond(secondValue));
                                    break;
                                case DT_DATETIME:
                                    int datetimeValue = ((BasicDateTime)sourceValue).getInt();
                                    ((BasicDateTimeVector)newVector).set(0, new BasicDateTime(datetimeValue));
                                    break;
                                case DT_TIMESTAMP:
                                    long timestampValue = ((BasicTimestamp)sourceValue).getLong();
                                    ((BasicTimestampVector)newVector).set(0, new BasicTimestamp(timestampValue));
                                    break;
                                case DT_NANOTIME:
                                    long nanotimeValue = ((BasicNanoTime)sourceValue).getLong();
                                    ((BasicNanoTimeVector)newVector).set(0, new BasicNanoTime(nanotimeValue));
                                    break;
                                case DT_NANOTIMESTAMP:
                                    long nanotimestampValue = ((BasicNanoTimestamp)sourceValue).getLong();
                                    ((BasicNanoTimestampVector)newVector).set(0, new BasicNanoTimestamp(nanotimestampValue));
                                    break;
                                // 可以根据需要添加更多类型
                                default:
                                    // 对于不认识的类型，直接复制原始值
                                    newVector.set(0, sourceValue);
                            }

                            newColumns[j-2] = newVector;
                        }

                        // 直接添加到表中
                        boolean appended = appendColumns(result, newColumns);
                        if (!appended) {
                            throw new RuntimeException("updateStreamingSQLResult failed when appending new data");
                        }

                        log.debug("成功添加新行，当前表行数: " + result.rows());

                        // 获取逻辑插入位置
                        int logicalInsertPos = ((BasicInt)insertNo.get(0)).getInt();
                        log.debug("逻辑插入位置: " + logicalInsertPos);

                        // 计算物理插入位置（考虑deleteLineMap）
                        int physicalInsertPos = logicalInsertPos;
                        int lineMapSize = wrapper.map != null ? wrapper.map.rows() : 0;
                        int[] mapArray = wrapper.map != null ? wrapper.map.getdataArray() : new int[0];

                        if (lineMapSize > 0) {
                            // 找到在deleteLineMap中小于logicalInsertPos的元素个数
                            int mapPos = findLowerBoundCount(mapArray, lineMapSize, logicalInsertPos);
                            physicalInsertPos = logicalInsertPos - mapPos;

                            // 更新deleteLineMap：在插入位置之后的所有删除记录都需要+1
                            for (int j = mapPos; j < lineMapSize; j++) {
                                mapArray[j]++;
                            }
                            wrapper.map = new BasicIntVector(mapArray);

                            log.debug("考虑deleteLineMap后，物理插入位置: " + physicalInsertPos +
                                    ", mapPos: " + mapPos);
                        }

                        // 边界检查
                        if (physicalInsertPos > prevSize) {
                            physicalInsertPos = prevSize;
                        } else if (physicalInsertPos < 0) {
                            physicalInsertPos = 0;
                        }

                        log.debug("最终物理插入位置: " + physicalInsertPos);

                        // 只有当插入位置不是在末尾时，才需要重排序
                        if (physicalInsertPos < prevSize) {
                            // 创建一个表示当前表行顺序的向量
                            BasicIntVector sortIndex = new BasicIntVector(result.rows());
                            int[] sortArray = sortIndex.getdataArray();

                            // 初始化排序数组
                            for (int j = 0; j < result.rows(); j++) {
                                sortArray[j] = j;
                            }

                            // 将最后一行（新插入的行）移动到指定位置
                            int lastRowIndex = prevSize; // 新插入行的索引

                            // 向右移动元素，为新行腾出空间
                            for (int j = lastRowIndex; j > physicalInsertPos; j--) {
                                sortArray[j] = sortArray[j - 1];
                            }
                            sortArray[physicalInsertPos] = lastRowIndex;

                            // 使用排序索引创建新表
                            List<Vector> newCols = new ArrayList<>();
                            for (int col = 0; col < result.columns(); col++) {
                                newCols.add(result.getColumn(col).getSubVector(sortArray));
                            }
                            result = new BasicTable(getColumnNames(result), newCols);

                            log.debug("完成单行插入重排序");
                        }
                    } else {
                        // 对于多行数据的处理
                        getLinesInLog(updateColumns, updateValues, updateLogSize, offset, length);

                        // 添加到表中
                        boolean appended = appendColumns(result, updateValues);
                        if (!appended) {
                            throw new RuntimeException("updateStreamingSQLResult failed with error: " + err);
                        }

                        log.debug("成功添加新行，当前表行数: " + result.rows());

                        // 计算排序索引
                        BasicIntVector sortIndex = insertIndexMapping(prevSize, insertNo, wrapper);

                        // 使用排序索引创建新表
                        List<Vector> newCols = new ArrayList<>();
                        for (int col = 0; col < result.columns(); col++) {
                            newCols.add(result.getColumn(col).getSubVector(sortIndex.getdataArray()));
                        }

                        // 创建并替换为新表 - 确保列顺序正确
                        List<String> columnNames = getColumnNames(result);
                        result = new BasicTable(columnNames, newCols);

                        log.debug("重排序后，表有 " + result.rows() + " 行");
                    }

                    log.debug("处理完成，当前表行数: " + result.rows());
                }
            }
        }

        log.debug("Completed updateStreamingSQLResult, table rows: " + result.rows());
        // Return updated table and deleteLineMap
        return new StreamingSQLResult(result, wrapper.map);
    }

    private static void getLinesInLog(List<Vector> updateColumns, Vector[] updateValues, int updateLogSize, int offset, int length) {
        for (int i = 0; i < updateColumns.size(); i++) {
            // 问题很可能出在这里：需要确保创建新的向量副本而不是共享引用
            Vector sourceVector;
            if (offset == 0 && updateLogSize == length) {
                sourceVector = updateColumns.get(i);
            } else {
                sourceVector = ((AbstractVector)updateColumns.get(i)).getSubVector(createRangeIndices(offset, length));
            }

            // 创建一个新的向量并复制数据，避免共享引用
            Entity.DATA_TYPE dataType = sourceVector.getDataType();
            Vector newVector = BasicEntityFactory.instance().createVectorWithDefaultValue(dataType, sourceVector.rows(), -1);

            // 逐个复制数据元素
            for (int j = 0; j < sourceVector.rows(); j++) {
                try {
                    Scalar value = (Scalar)sourceVector.get(j);
                    newVector.set(j, value);
                } catch (Exception e) {
                    log.error("Error copying data: " + e.getMessage());
                }
            }

            updateValues[i] = newVector;

            // 调试日志
            log.debug("Created new vector of type " + dataType + " with " + newVector.rows() + " rows");
            for (int j = 0; j < Math.min(5, newVector.rows()); j++) {
                log.debug("Element " + j + ": " + newVector.get(j));
            }
        }
    }

    // Helper function: Create consecutive index array
    private static int[] createRangeIndices(int start, int length) {
        int[] indices = new int[length];
        for (int i = 0; i < length; i++) {
            indices[i] = start + i;
        }
        return indices;
    }

    // Helper function: Get column names list from table
    private static List<String> getColumnNames(BasicTable table) {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < table.columns(); i++) {
            names.add(table.getColumnName(i));
        }
        return names;
    }

    // Binary search function
    private static int findLowerBoundCount(int[] data, int size, int value) {
        int start = 0;
        int count = size - start;
        if (size > 0 && data[size - 1] < value) {
            return size;
        }
        if (size > 0 && data[0] > value) {
            return 0;
        }
        while (count > 0) {
            int step = count / 2;
            int index = start + step;
            if (data[index] < value) {
                start = index + 1;
                count -= step + 1;
            } else {
                count = step;
            }
        }
        return start;
    }

    // Insert into sorted array
    private static int[] insertSortedVec(BasicIntVector vec, int[] data, int size) {
        int start = vec.rows();
        int[] currentArray = vec.getdataArray();
        int[] newArray = Arrays.copyOf(currentArray, start + size);

        int cur;
        int curIndex;
        for (int i = 0; i < size; i++) {
            cur = data[i];
            curIndex = i + start - 1;
            while (curIndex >= 0 && newArray[curIndex] > cur) {
                newArray[curIndex + 1] = newArray[curIndex];
                curIndex--;
            }
            newArray[curIndex + 1] = cur;
        }

        return newArray;
    }

    // Update sorted array
    private static void updateSortedVec(int[] indexArray, int size, int pos) {
        for (int i = pos; i < size; i++) {
            indexArray[i] += 1;
        }
    }

    // Process delete operation
    private static void removeIndexMapping(Vector removeIndex, int prevSize, DeleteLineMapWrapper wrapper) throws Exception {
        int length = removeIndex.rows();
        int start = 0;
        int lineMapSize = wrapper.map != null ? wrapper.map.rows() : 0;
        List<Integer> logicalDeleteList = new ArrayList<>(length);
        int[] mapArray = wrapper.map != null ? wrapper.map.getdataArray() : new int[0];

        // Special handling: If table is empty, skip processing
        if (prevSize == 0) {
            log.debug("Table is empty, skipping delete operation in removeIndexMapping");
            return;
        }

        while (start < length) {
            int count = Math.min(1024, length - start);
            int[] pindex = new int[count];
            for (int i = 0; i < count; i++) {
                pindex[i] = ((BasicInt)removeIndex.get(start + i)).getInt();
            }

            for (int i = 0; i < count; ++i) {
                logicalDeleteList.add(pindex[i]);
                if (lineMapSize > 0) {
                    pindex[i] -= findLowerBoundCount(mapArray, lineMapSize, pindex[i]);
                    if (pindex[i] >= prevSize) {
                        // Don't throw exception, just log warning
                        log.debug("Warning: Adjusting deleteLogNo from " + pindex[i] + " to " + (prevSize - 1));
                        pindex[i] = prevSize - 1; // Adjust out-of-range index to last row
                    }
                }
            }

            // Update removeIndex
            for (int i = 0; i < count; i++) {
                ((BasicIntVector)removeIndex).set(start + i, new BasicInt(pindex[i]));
            }
            start += count;
        }

        // Convert List to array
        int[] deleteArray = new int[logicalDeleteList.size()];
        for (int i = 0; i < logicalDeleteList.size(); i++) {
            deleteArray[i] = logicalDeleteList.get(i);
        }

        // Insert into deleteLineMap and update wrapper
        int[] newArray = insertSortedVec(wrapper.map, deleteArray, deleteArray.length);
        wrapper.map = new BasicIntVector(newArray);
    }

    // Process update operation
    private static void updateIndexMapping(Vector updateIndex, int prevSize, DeleteLineMapWrapper wrapper) throws Exception {
        int length = updateIndex.rows();
        int start = 0;
        int lineMapSize = wrapper.map != null ? wrapper.map.rows() : 0;
        int[] mapArray = wrapper.map != null ? wrapper.map.getdataArray() : new int[0];

        // Special handling: If table is empty, allow index 0
        if (prevSize == 0) {
            log.debug("Table is empty, skipping index validation in updateIndexMapping");
            return;
        }

        if (lineMapSize > 0) {
            while (start < length) {
                int count = Math.min(1024, length - start);
                int[] pindex = new int[count];
                for (int i = 0; i < count; i++) {
                    pindex[i] = ((BasicInt)updateIndex.get(start + i)).getInt();
                }

                for (int i = 0; i < count; ++i) {
                    pindex[i] -= findLowerBoundCount(mapArray, lineMapSize, pindex[i]);
                    if (pindex[i] >= prevSize) {
                        // Don't throw exception, just log warning
                        log.debug("Warning: Adjusting updateLogNo from " + pindex[i] + " to " + (prevSize - 1));
                        pindex[i] = prevSize - 1; // Adjust out-of-range index to last row
                    }
                }

                // Update updateIndex
                for (int i = 0; i < count; i++) {
                    ((BasicIntVector)updateIndex).set(start + i, new BasicInt(pindex[i]));
                }
                start += count;
            }
        } else {
            while (start < length) {
                int count = Math.min(1024, length - start);
                for (int i = 0; i < count; ++i) {
                    int value = ((BasicInt)updateIndex.get(start + i)).getInt();
                    if (value >= prevSize) {
                        // Don't throw exception, just log warning and adjust index
                        log.debug("Warning: Adjusting updateLogNo from " + value + " to " + (prevSize - 1));
                        ((BasicIntVector)updateIndex).set(start + i, new BasicInt(prevSize - 1));
                    }
                }
                start += count;
            }
        }
    }

    // Process insert operation
    private static BasicIntVector insertIndexMapping(int prevSize, Vector insertIndex, DeleteLineMapWrapper wrapper) {
        int length = insertIndex.rows();
        BasicIntVector sortIndex = new BasicIntVector(prevSize + length);
        int[] psort = sortIndex.getdataArray();

        // 获取原始的deleteLineMap数组
        int[] originalMapArray = wrapper.map != null ? wrapper.map.getdataArray() : new int[0];
        // 复制deleteLineMap，因为我们需要在每次插入时更新它
        int[] mapArray = originalMapArray.clone();
        int lineMapSize = mapArray.length;

        // 初始化排序数组：前prevSize个元素为0到prevSize-1，后length个元素为prevSize到prevSize+length-1
        for (int i = 0; i < prevSize + length; i++) {
            psort[i] = i;
        }

        log.debug("insertIndexMapping开始: 原始大小=" + prevSize + ", 插入索引长度=" + length);
        if (lineMapSize > 0) {
            log.debug("deleteLineMap大小: " + lineMapSize);
        }

        // 按顺序处理每个插入操作
        for (int i = 0; i < length; i++) {
            int logicalInsertPos = ((BasicInt)insertIndex.get(i)).getInt();
            int currIndex = i + prevSize; // 新行在扩展后表中的实际索引位置

            // 计算物理插入位置（考虑已删除的行）
            int physicalInsertPos = logicalInsertPos;
            if (lineMapSize > 0) {
                // 找到在deleteLineMap中小于logicalInsertPos的元素个数
                int mapPos = findLowerBoundCount(mapArray, lineMapSize, logicalInsertPos);
                physicalInsertPos = logicalInsertPos - mapPos;

                // 更新deleteLineMap：在插入位置之后的所有删除记录都需要+1
                // 因为我们插入了一个新行，影响了后续的逻辑行号
                for (int j = mapPos; j < lineMapSize; j++) {
                    mapArray[j]++;
                }

                log.debug("第 " + i + " 个插入: 逻辑位置=" + logicalInsertPos +
                        ", 映射位置=" + mapPos + ", 物理位置=" + physicalInsertPos);
            }

            // 边界检查：确保物理插入位置在有效范围内
            if (physicalInsertPos > prevSize + i) {
                log.debug("Warning: 调整物理插入位置从 " + physicalInsertPos + " 到 " + (prevSize + i));
                physicalInsertPos = prevSize + i;
            } else if (physicalInsertPos < 0) {
                log.debug("Warning: 调整物理插入位置从 " + physicalInsertPos + " 到 0");
                physicalInsertPos = 0;
            }

            log.debug("处理第 " + i + " 个插入: 逻辑位置=" + logicalInsertPos +
                    ", 最终物理位置=" + physicalInsertPos + ", 当前索引=" + currIndex);

            // 将当前行插入到指定位置
            // 需要将physicalInsertPos到currIndex-1之间的所有元素向右移动一位
            for (int j = currIndex; j > physicalInsertPos; j--) {
                psort[j] = psort[j - 1];
            }

            // 在计算出的物理位置插入新行
            psort[physicalInsertPos] = currIndex;
        }

        // 打印最终排序索引用于调试
        StringBuilder sb = new StringBuilder("最终排序索引: [");
        for (int i = 0; i < Math.min(20, sortIndex.rows()); i++) {
            if (i > 0) sb.append(", ");
            sb.append(psort[i]);
        }
        if (sortIndex.rows() > 20) sb.append(", ...");
        sb.append("]");
        log.debug(sb.toString());

        // 验证排序索引的有效性
        boolean[] used = new boolean[psort.length];
        boolean isValid = true;
        for (int i = 0; i < psort.length; i++) {
            if (psort[i] < 0 || psort[i] >= psort.length) {
                System.err.println("Invalid sort index at position " + i + ": " + psort[i]);
                isValid = false;
                break;
            }
            if (used[psort[i]]) {
                System.err.println("Duplicate sort index: " + psort[i]);
                isValid = false;
                break;
            }
            used[psort[i]] = true;
        }

        if (!isValid) {
            System.err.println("排序索引无效，重置为默认顺序");
            for (int j = 0; j < psort.length; j++) {
                psort[j] = j;
            }
        }

        // 更新wrapper中的deleteLineMap
        if (lineMapSize > 0) {
            wrapper.map = new BasicIntVector(mapArray);
            log.debug("更新后的deleteLineMap大小: " + wrapper.map.rows());
        }

        return sortIndex;
    }

    // Helper function: Remove rows
    private static BasicTable removeRows(BasicTable table, BasicIntVector rowIndices) {
        try {
            if (table.rows() == 0 || rowIndices == null || rowIndices.rows() == 0) {
                return table; // 没有行需要删除
            }

            // 获取要保留的行的索引
            int[] keepIndices = createComplementIndices(table.rows(), rowIndices);

            // 获取列名
            List<String> columnNames = getColumnNames(table);

            if (keepIndices.length == 0) {
                // 所有行都被删除，创建空表
                List<Vector> emptyColumns = new ArrayList<>();
                for (int i = 0; i < table.columns(); i++) {
                    emptyColumns.add(BasicEntityFactory.instance().createVectorWithDefaultValue(
                            table.getColumn(i).getDataType(), 0, -1));
                }
                return new BasicTable(columnNames, emptyColumns);
            } else {
                // 有行需要保留，创建子表（保留原始顺序）
                List<Vector> newColumns = new ArrayList<>();
                for (int i = 0; i < table.columns(); i++) {
                    newColumns.add(table.getColumn(i).getSubVector(keepIndices));
                }
                return new BasicTable(columnNames, newColumns);
            }
        } catch (Exception e) {
            log.error("Error in removeRows: " + e.getMessage(), e);
            return table; // 出错时返回原表
        }
    }


    // Helper function: Create complement indices
    private static int[] createComplementIndices(int totalRows, BasicIntVector excludeIndices) {
        boolean[] exclude = new boolean[totalRows];
        for (int i = 0; i < excludeIndices.rows(); i++) {
            int idx = ((BasicInt)excludeIndices.get(i)).getInt();
            if (idx >= 0 && idx < totalRows) {
                exclude[idx] = true;
            }
        }

        int count = 0;
        for (boolean ex : exclude) {
            if (!ex) count++;
        }

        int[] result = new int[count];
        int pos = 0;
        for (int i = 0; i < totalRows; i++) {
            if (!exclude[i]) {
                result[pos++] = i;
            }
        }
        return result;
    }

    // Helper function: Append columns
    private static boolean appendColumns(BasicTable table, Vector[] columns) {
        try {
            // If all columns are empty, return true directly
            if (columns.length == 0) {
                log.debug("No columns to append");
                return true;
            }

            // Get number of rows to add
            int rowsToAdd = columns[0].rows();
            log.debug("Appending " + rowsToAdd + " rows to table with " + table.columns() + " columns");

            // Ensure all columns have the same number of rows
            for (int i = 0; i < columns.length; i++ ) {
                if (columns[i].rows() != rowsToAdd && !(table.getColumn(i) instanceof BasicArrayVector) && !(table.getColumn(i) instanceof BasicAnyVector)) {
                    throw new IllegalArgumentException("All columns must have the same number of rows");
                }
            }

            // Add rows to table
            for (int col = 0; col < columns.length; col++) {
                table.getColumn(col).Append(columns[col]);
            }

            log.debug("After append, table has " + table.rows() + " rows");
            return true;
        } catch (Exception e) {
            System.err.println("Error in appendColumns: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    // Helper function: Update rows
    private static boolean updateRows(BasicTable table, Vector[] values, BasicIntVector rowIndices, List<String> columnNames) {
        try {
            // If table is empty and it's an update operation, convert to append
            if (table.rows() == 0 && rowIndices.rows() > 0) {
                log.debug("Table is empty, converting update to append");
                return appendColumns(table, values);
            }

            // Get column name to index mapping
            Map<String, Integer> colNameToIndex = new HashMap<>();
            for (int i = 0; i < columnNames.size(); i++) {
                colNameToIndex.put(columnNames.get(i), i);
            }

            // For each row to update
            for (int i = 0; i < rowIndices.rows(); i++) {
                int rowIndex = ((BasicInt)rowIndices.get(i)).getInt();

                // Ensure row index is within valid range
                if (rowIndex < 0 || rowIndex >= table.rows()) {
                    log.debug("Row index " + rowIndex + " out of bounds, skipping");
                    continue;
                }

                // Update each column in the row
                for (int colIndex = 0; colIndex < values.length; colIndex++) {
                    // Get value
                    Entity value = values[colIndex].get(i);

                    // Get table column index
                    int tableColIndex = colIndex;
                    if (tableColIndex >= 0 && tableColIndex < table.columns()) {
                        // Update cell
                        table.getColumn(tableColIndex).set(rowIndex, value);
                    }
                }
            }

            return true;
        } catch (Exception e) {
            System.err.println("Error in updateRows: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
}