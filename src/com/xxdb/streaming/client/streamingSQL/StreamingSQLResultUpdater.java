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
    protected static class StreamingSQLResult {
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
    protected static StreamingSQLResult updateStreamingSQLResult(BasicTable result, BasicIntVector deleteLineMap, IMessage msg) throws Exception {
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
                                    log.debug("=== PROCESSING kUpdate ===");
                                    log.debug("Table rows: " + result.rows());

                                    List<String> columnNames = new ArrayList<>();
                                    for (int j = 0; j < result.columns(); ++j) {
                                        columnNames.add(result.getColumnName(j));
                                    }

                                    // Get updated row indices
                                    BasicIntVector updateLineNo = (BasicIntVector)((AbstractVector)lineNoColumn).getSubVector(createRangeIndices(offset, length));
                                    log.debug("Original logical line numbers: " + updateLineNo.getString());

                                    // First perform the mapping conversion from logical row numbers to physical row numbers
                                    updateIndexMapping(updateLineNo, result.rows(), wrapper);
                                    log.debug("After mapping to physical line numbers: " + updateLineNo.getString());

                                    List<Integer> validIndices = new ArrayList<>();

                                    for (int idx = 0; idx < updateLineNo.rows(); idx++) {
                                        int physicalLineNo = ((BasicInt)updateLineNo.get(idx)).getInt();
                                        log.debug("Checking physical lineNo " + physicalLineNo + " against table size " + result.rows());
                                        if (physicalLineNo >= 0 && physicalLineNo < result.rows()) {
                                            validIndices.add(idx);
                                            log.debug("  -> Valid physical index: " + physicalLineNo);
                                        } else {
                                            log.debug("  -> Invalid physical index: " + physicalLineNo + " (table size: " + result.rows() + ")");
                                            log.debug("Warning: Mapped physical index " + physicalLineNo + " out of range, skipping");
                                        }
                                    }

                                    log.debug("Valid indices count: " + validIndices.size());

                                    // Process valid update indices only
                                    if (!validIndices.isEmpty()) {
                                        log.debug("Calling updateRows with valid physical indices");
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

                                            // Check table column types to determine how to process the data
                                            Vector tableColumn = result.getColumn(colIdx);
                                            if (tableColumn instanceof BasicArrayVector) {
                                                log.debug("Column " + colIdx + " is BasicArrayVector, using original vector");
                                                validUpdateValues[colIdx] = updateValues[colIdx];
                                            } else {
                                                log.debug("Column " + colIdx + " is not BasicArrayVector, using getSubVector");
                                                validUpdateValues[colIdx] = ((AbstractVector)updateValues[colIdx]).getSubVector(validIndicesArray);
                                            }
                                        }

                                        // Note: Do not call updateIndexMapping again here, as the conversion has already been done.
                                        boolean updated = updateRows(result, validUpdateValues, validUpdateLineNo, columnNames);
                                        if (!updated) {
                                            throw new RuntimeException("updateStreamingSQLResult failed with error: " + err);
                                        }
                                    } else {
                                        log.debug("No valid physical indices found after mapping, updateRows not called!");
                                    }

                                    log.debug("=== END PROCESSING kUpdate ===");
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
                                // Sort first, then delete
                                int[] originalArray = removeLineNo.getdataArray();
                                int[] sortedArray = originalArray.clone();
                                Arrays.sort(sortedArray);
                                removeLineNo = new BasicIntVector(sortedArray);

                                if (result.rows() > 0) {
                                    result = removeRows(result, removeLineNo);
                                }
                            } else if (prevUpdateType == LogType.kInsert) {
                                // Process insert operation
                                log.debug("Process kInsert type messages");

                                // Get insertNo
                                BasicIntVector insertNo = (BasicIntVector)((AbstractVector)lineNoColumn).getSubVector(createRangeIndices(offset, length));
                                int prevSize = result.rows();

                                log.debug("msg content: type=" + prevUpdateType + ", lineNo=" + insertNo.getString() + ", cols=" + (msg.size() - 2));

                                // Process single-row data
                                if (insertNo.rows() == 1) {
                                    // Create a column array containing new data, excluding the type and row number columns.
                                    Vector[] newColumns = new Vector[msg.size() - 2];

                                    // Create a vector of the corresponding type for each column
                                    for (int j = 2; j < msg.size(); j++) {
                                        if (msg.getEntity(j) instanceof Vector) {
                                            newColumns[j-2] = (Vector) msg.getEntity(j);
                                        } else {
                                            Scalar sourceValue = (Scalar) msg.getEntity(j);
                                            Entity.DATA_TYPE dataType = sourceValue.getDataType();

                                            Vector newVector;
                                            if (msg.getEntity(j) instanceof BasicDecimal32 || msg.getEntity(j) instanceof BasicDecimal64 || msg.getEntity(j) instanceof BasicDecimal128) {
                                                newVector = BasicEntityFactory.instance().createVectorWithDefaultValue(dataType, 1, ((Scalar) msg.getEntity(j)).getScale());
                                            } else {
                                                newVector = BasicEntityFactory.instance().createVectorWithDefaultValue(dataType, 1, -1);
                                            }

                                            newVector.set(0, sourceValue);
                                            newColumns[j-2] = newVector;
                                        }
                                    }

                                    // Add directly to the table
                                    boolean appended = appendColumns(result, newColumns);
                                    if (!appended) {
                                        throw new RuntimeException("updateStreamingSQLResult failed when appending new data");
                                    }

                                    log.debug("Successfully added new row, current table row count: " + result.rows());

                                    // Get the logical insertion position.
                                    int logicalInsertPos = ((BasicInt)insertNo.get(0)).getInt();
                                    log.debug("logical insertion position: " + logicalInsertPos);

                                    // Calculate the physical insertion position (considering deleteLineMap).
                                    int physicalInsertPos = logicalInsertPos;
                                    int lineMapSize = wrapper.map != null ? wrapper.map.rows() : 0;
                                    int[] mapArray = wrapper.map != null ? wrapper.map.getdataArray() : new int[0];

                                    if (lineMapSize > 0) {
                                        // Find the number of elements in deleteLineMap that are less than logicalInsertPos
                                        int mapPos = findLowerBoundCount(mapArray, lineMapSize, logicalInsertPos);
                                        physicalInsertPos = logicalInsertPos - mapPos;

                                        // Update deleteLineMap: All deletion records after the insertion position need to be incremented by 1
                                        for (int j = mapPos; j < lineMapSize; j++) {
                                            mapArray[j]++;
                                        }
                                        wrapper.map = new BasicIntVector(mapArray);

                                        log.debug("After considering deleteLineMap, the physical insertion position: " + physicalInsertPos + ", mapPos: " + mapPos);
                                    }

                                    // Boundary check
                                    if (physicalInsertPos > prevSize) {
                                        physicalInsertPos = prevSize;
                                    } else if (physicalInsertPos < 0) {
                                        physicalInsertPos = 0;
                                    }

                                    log.debug("Final physical insertion position.: " + physicalInsertPos);

                                    // Reordering is only necessary if the insertion position is not at the end
                                    if (physicalInsertPos < prevSize) {
                                        // Create a vector representing the current row order of the table
                                        BasicIntVector sortIndex = new BasicIntVector(result.rows());
                                        int[] sortArray = sortIndex.getdataArray();

                                        // Initialize the sorting array
                                        for (int j = 0; j < result.rows(); j++) {
                                            sortArray[j] = j;
                                        }

                                        // Move the last row (the newly inserted row) to the specified position, indicating the index of the newly inserted row
                                        int lastRowIndex = prevSize;

                                        // Shift elements to the right to make space for the new row
                                        for (int j = lastRowIndex; j > physicalInsertPos; j--) {
                                            sortArray[j] = sortArray[j - 1];
                                        }
                                        sortArray[physicalInsertPos] = lastRowIndex;

                                        // Create a new table using sorted indices
                                        List<Vector> newCols = new ArrayList<>();
                                        for (int col = 0; col < result.columns(); col++) {
                                            newCols.add(result.getColumn(col).getSubVector(sortArray));
                                        }
                                        result = new BasicTable(getColumnNames(result), newCols);

                                        log.debug("Completed single-row insertion reordering");
                                    }
                                } else {
                                    // Process multi-rows
                                    getLinesInLog(updateColumns, updateValues, updateLogSize, offset, length);

                                    // Add directly to the table
                                    boolean appended = appendColumns(result, updateValues);
                                    if (!appended) {
                                        throw new RuntimeException("updateStreamingSQLResult failed with error: " + err);
                                    }

                                    log.debug("Successfully added new row, current table row count: " + result.rows());

                                    // Calculate the sorting indices
                                    BasicIntVector sortIndex = insertIndexMapping(prevSize, insertNo, wrapper);

                                    // Create a new table using sorted indices
                                    List<Vector> newCols = new ArrayList<>();
                                    for (int col = 0; col < result.columns(); col++) {
                                        newCols.add(result.getColumn(col).getSubVector(sortIndex.getdataArray()));
                                    }

                                    // Create and replace with a new table - ensure the column order is correct
                                    List<String> columnNames = getColumnNames(result);
                                    result = new BasicTable(columnNames, newCols);

                                    log.debug("After reordering, the table has " + result.rows() + " rows");
                                }

                                log.debug("Processing completed, current table row count: " + result.rows());
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
                        log.debug("Table is empty, treating kUpdate as kAppend");
                        boolean appended = appendColumns(result, updateValues);
                        if (!appended) {
                            throw new RuntimeException("updateStreamingSQLResult failed with error: " + err);
                        }
                    } else {
                        log.debug("=== PROCESSING kUpdate ===");
                        log.debug("Table rows: " + result.rows());

                        List<String> columnNames = new ArrayList<>();
                        for (int j = 0; j < result.columns(); ++j) {
                            columnNames.add(result.getColumnName(j));
                        }

                        // Get updated row indices
                        BasicIntVector updateLineNo = (BasicIntVector)((AbstractVector)lineNoColumn).getSubVector(createRangeIndices(offset, length));
                        log.debug("Original logical line numbers: " + updateLineNo.getString());

                        // First perform the mapping conversion from logical row numbers to physical row numbers.
                        updateIndexMapping(updateLineNo, result.rows(), wrapper);
                        log.debug("After mapping to physical line numbers: " + updateLineNo.getString());

                        // Now verify whether the converted physical row number is valid.
                        List<Integer> validIndices = new ArrayList<>();

                        for (int idx = 0; idx < updateLineNo.rows(); idx++) {
                            int physicalLineNo = ((BasicInt)updateLineNo.get(idx)).getInt();
                            log.debug("Checking physical lineNo " + physicalLineNo + " against table size " + result.rows());
                            if (physicalLineNo >= 0 && physicalLineNo < result.rows()) {
                                validIndices.add(idx);
                                log.debug("  -> Valid physical index: " + physicalLineNo);
                            } else {
                                log.debug("  -> Invalid physical index: " + physicalLineNo + " (table size: " + result.rows() + ")");
                                log.debug("Warning: Mapped physical index " + physicalLineNo + " out of range, skipping");
                            }
                        }

                        log.debug("Valid indices count: " + validIndices.size());

                        // Process valid update indices only
                        if (!validIndices.isEmpty()) {
                            log.debug("Calling updateRows with valid physical indices");
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

                                // Check table column types to determine how to process the data.
                                Vector tableColumn = result.getColumn(colIdx);
                                if (tableColumn instanceof BasicArrayVector) {
                                    log.debug("Column " + colIdx + " is BasicArrayVector, using original vector");
                                    validUpdateValues[colIdx] = updateValues[colIdx];
                                } else {
                                    log.debug("Column " + colIdx + " is not BasicArrayVector, using getSubVector");
                                    validUpdateValues[colIdx] = ((AbstractVector)updateValues[colIdx]).getSubVector(validIndicesArray);
                                }
                            }

                            // Note: Do not call updateIndexMapping again here, as the conversion has already been done.
                            boolean updated = updateRows(result, validUpdateValues, validUpdateLineNo, columnNames);
                            if (!updated) {
                                throw new RuntimeException("updateStreamingSQLResult failed with error: " + err);
                            }
                        } else {
                            log.debug("No valid physical indices found after mapping, updateRows not called!");
                        }

                        log.debug("=== END PROCESSING kUpdate ===");
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
                    // Sort first, then delete
                    int[] originalArray = removeLineNo.getdataArray();
                    int[] sortedArray = originalArray.clone();
                    Arrays.sort(sortedArray);
                    removeLineNo = new BasicIntVector(sortedArray);

                    if (result.rows() > 0) {
                        result = removeRows(result, removeLineNo);
                    }
                } else if (prevUpdateType == LogType.kInsert) {
                    // Process insert operation
                    log.debug("Process kInsert type messages");

                    // Get insertNo
                    BasicIntVector insertNo = (BasicIntVector)((AbstractVector)lineNoColumn).getSubVector(createRangeIndices(offset, length));
                    int prevSize = result.rows();

                    log.debug("msg content: type=" + prevUpdateType + ", lineNo=" + insertNo.getString() + ", cols=" + (msg.size() - 2));

                    // Process single-row data
                    if (insertNo.rows() == 1) {
                        // Create a column array containing new data, excluding the type and row number columns.
                        Vector[] newColumns = new Vector[msg.size() - 2];

                        // Create a vector of the corresponding type for each column
                        for (int j = 2; j < msg.size(); j++) {
                            if (msg.getEntity(j) instanceof Vector) {
                                newColumns[j-2] = (Vector) msg.getEntity(j);
                            } else {
                                Scalar sourceValue = (Scalar) msg.getEntity(j);
                                Entity.DATA_TYPE dataType = sourceValue.getDataType();

                                Vector newVector;
                                if (msg.getEntity(j) instanceof BasicDecimal32 || msg.getEntity(j) instanceof BasicDecimal64 || msg.getEntity(j) instanceof BasicDecimal128) {
                                    newVector = BasicEntityFactory.instance().createVectorWithDefaultValue(dataType, 1, ((Scalar) msg.getEntity(j)).getScale());
                                } else {
                                    newVector = BasicEntityFactory.instance().createVectorWithDefaultValue(dataType, 1, -1);
                                }

                                newVector.set(0, sourceValue);
                                newColumns[j-2] = newVector;
                            }
                        }

                        // Add directly to the table.
                        boolean appended = appendColumns(result, newColumns);
                        if (!appended) {
                            throw new RuntimeException("updateStreamingSQLResult failed when appending new data");
                        }

                        log.debug("Successfully added new row, current table row count: " + result.rows());

                        // Get the logical insertion position
                        int logicalInsertPos = ((BasicInt)insertNo.get(0)).getInt();
                        log.debug("logical insertion position: " + logicalInsertPos);

                        // Calculate the physical insertion position (considering deleteLineMap)
                        int physicalInsertPos = logicalInsertPos;
                        int lineMapSize = wrapper.map != null ? wrapper.map.rows() : 0;
                        int[] mapArray = wrapper.map != null ? wrapper.map.getdataArray() : new int[0];

                        if (lineMapSize > 0) {
                            // Find the number of elements in deleteLineMap that are less than logicalInsertPos
                            int mapPos = findLowerBoundCount(mapArray, lineMapSize, logicalInsertPos);
                            physicalInsertPos = logicalInsertPos - mapPos;

                            // Update deleteLineMap: All deletion records after the insertion position need to be incremented by 1
                            for (int j = mapPos; j < lineMapSize; j++) {
                                mapArray[j]++;
                            }
                            wrapper.map = new BasicIntVector(mapArray);

                            log.debug("After considering deleteLineMap, the physical insertion position: " + physicalInsertPos + ", mapPos: " + mapPos);
                        }

                        // Boundary check
                        if (physicalInsertPos > prevSize) {
                            physicalInsertPos = prevSize;
                        } else if (physicalInsertPos < 0) {
                            physicalInsertPos = 0;
                        }

                        log.debug("Final physical insertion position.: " + physicalInsertPos);

                        // Reordering is only necessary if the insertion position is not at the end
                        if (physicalInsertPos < prevSize) {
                            // Create a vector representing the current row order of the table
                            BasicIntVector sortIndex = new BasicIntVector(result.rows());
                            int[] sortArray = sortIndex.getdataArray();

                            // Initialize the sorting array
                            for (int j = 0; j < result.rows(); j++) {
                                sortArray[j] = j;
                            }

                            // Move the last row (the newly inserted row) to the specified position, indicating the index of the newly inserted row.
                            int lastRowIndex = prevSize;

                            // Shift elements to the right to make space for the new row
                            for (int j = lastRowIndex; j > physicalInsertPos; j--) {
                                sortArray[j] = sortArray[j - 1];
                            }
                            sortArray[physicalInsertPos] = lastRowIndex;

                            // Create a new table using sorted indices
                            List<Vector> newCols = new ArrayList<>();
                            for (int col = 0; col < result.columns(); col++) {
                                newCols.add(result.getColumn(col).getSubVector(sortArray));
                            }
                            result = new BasicTable(getColumnNames(result), newCols);

                            log.debug("Completed single-row insertion reordering");
                        }
                    } else {
                        // Process multi-rows
                        getLinesInLog(updateColumns, updateValues, updateLogSize, offset, length);

                        boolean appended = appendColumns(result, updateValues);
                        if (!appended) {
                            throw new RuntimeException("updateStreamingSQLResult failed with error: " + err);
                        }

                        log.debug("Successfully added new row, current table row count: " + result.rows());

                        // Calculate the sorting indices
                        BasicIntVector sortIndex = insertIndexMapping(prevSize, insertNo, wrapper);

                        // Create a new table using sorted indices
                        List<Vector> newCols = new ArrayList<>();
                        for (int col = 0; col < result.columns(); col++) {
                            newCols.add(result.getColumn(col).getSubVector(sortIndex.getdataArray()));
                        }

                        // Create and replace with a new table - ensure the column order is correct.
                        List<String> columnNames = getColumnNames(result);
                        result = new BasicTable(columnNames, newCols);

                        log.debug("After reordering, the table has " + result.rows() + " rows");
                    }

                    log.debug("Processing completed, current table row count: " + result.rows());
                }
            }
        }

        log.debug("Completed updateStreamingSQLResult, table rows: " + result.rows());
        // Return updated table and deleteLineMap
        return new StreamingSQLResult(result, wrapper.map);
    }

    private static void getLinesInLog(List<Vector> updateColumns, Vector[] updateValues, int updateLogSize, int offset, int length) {
        for (int i = 0; i < updateColumns.size(); i++) {
            // It is necessary to ensure the creation of new vector copies rather than shared references
            Vector sourceVector;
            if (offset == 0 && updateLogSize == length) {
                sourceVector = updateColumns.get(i);
            } else {
                sourceVector = ((AbstractVector)updateColumns.get(i)).getSubVector(createRangeIndices(offset, length));
            }

            // Create a new vector and copy the data to avoid shared references
            Entity.DATA_TYPE dataType = sourceVector.getDataType();
            int scale = -1;
            if (dataType == Entity.DATA_TYPE.DT_DECIMAL32) {
                scale = ((BasicDecimal32Vector) sourceVector).getScale();
            } else if(dataType == Entity.DATA_TYPE.DT_DECIMAL64) {
                scale = ((BasicDecimal64Vector) sourceVector).getScale();
            } else if(dataType == Entity.DATA_TYPE.DT_DECIMAL128) {
                scale = ((BasicDecimal128Vector) sourceVector).getScale();
            }

            Vector newVector = BasicEntityFactory.instance().createVectorWithDefaultValue(dataType, sourceVector.rows(), scale);

            // Copy data elements one by one
            for (int j = 0; j < sourceVector.rows(); j++) {
                try {
                    Scalar value = (Scalar)sourceVector.get(j);
                    newVector.set(j, value);
                } catch (Exception e) {
                    log.error("Error copying data: " + e.getMessage());
                }
            }

            updateValues[i] = newVector;

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

        // Get original deleteLineMap array
        int[] originalMapArray = wrapper.map != null ? wrapper.map.getdataArray() : new int[0];
        // Copy deleteLineMap since we need to update it with each insertion
        int[] mapArray = originalMapArray.clone();
        int lineMapSize = mapArray.length;

        // Initialize sort array: first prevSize elements are 0 to prevSize-1, next length elements are prevSize to prevSize+length-1
        for (int i = 0; i < prevSize + length; i++) {
            psort[i] = i;
        }

        log.debug("insertIndexMapping start: original size=" + prevSize + ", insert index length=" + length);
        if (lineMapSize > 0) {
            log.debug("deleteLineMap size: " + lineMapSize);
        }

        // Process each insert operation in order
        for (int i = 0; i < length; i++) {
            int logicalInsertPos = ((BasicInt)insertIndex.get(i)).getInt();
            int currIndex = i + prevSize; // Actual index position of new row in extended table

            // Calculate physical insert position (considering deleted rows)
            int physicalInsertPos = logicalInsertPos;
            if (lineMapSize > 0) {
                // Find count of elements in deleteLineMap that are less than logicalInsertPos
                int mapPos = findLowerBoundCount(mapArray, lineMapSize, logicalInsertPos);
                physicalInsertPos = logicalInsertPos - mapPos;

                // Update deleteLineMap: all delete records after insert position need to be incremented by 1
                // Because we inserted a new row, affecting subsequent logical row numbers
                for (int j = mapPos; j < lineMapSize; j++) {
                    mapArray[j]++;
                }

                log.debug("Insert " + i + ": logical position=" + logicalInsertPos +
                        ", map position=" + mapPos + ", physical position=" + physicalInsertPos);
            }

            // Boundary check: ensure physical insert position is within valid range
            if (physicalInsertPos > prevSize + i) {
                log.debug("Warning: adjusting physical insert position from " + physicalInsertPos + " to " + (prevSize + i));
                physicalInsertPos = prevSize + i;
            } else if (physicalInsertPos < 0) {
                log.debug("Warning: adjusting physical insert position from " + physicalInsertPos + " to 0");
                physicalInsertPos = 0;
            }

            log.debug("Processing insert " + i + ": logical position=" + logicalInsertPos +
                    ", final physical position=" + physicalInsertPos + ", current index=" + currIndex);

            // Insert current row at specified position
            // Need to shift all elements from physicalInsertPos to currIndex-1 one position to the right
            for (int j = currIndex; j > physicalInsertPos; j--) {
                psort[j] = psort[j - 1];
            }

            // Insert new row at calculated physical position
            psort[physicalInsertPos] = currIndex;
        }

        // Print final sort index for debugging
        StringBuilder sb = new StringBuilder("Final sort index: [");
        for (int i = 0; i < Math.min(20, sortIndex.rows()); i++) {
            if (i > 0) sb.append(", ");
            sb.append(psort[i]);
        }
        if (sortIndex.rows() > 20) sb.append(", ...");
        sb.append("]");
        log.debug(sb.toString());

        // Validate sort index validity
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
            System.err.println("Sort index invalid, resetting to default order");
            for (int j = 0; j < psort.length; j++) {
                psort[j] = j;
            }
        }

        // Update deleteLineMap in wrapper
        if (lineMapSize > 0) {
            wrapper.map = new BasicIntVector(mapArray);
            log.debug("Updated deleteLineMap size: " + wrapper.map.rows());
        }

        return sortIndex;
    }

    // Helper function: Remove rows
    private static BasicTable removeRows(BasicTable table, BasicIntVector rowIndices) {
        try {
            if (table.rows() == 0 || rowIndices == null || rowIndices.rows() == 0) {
                return table; // No rows need to be removed
            }

            // Get indices of rows to keep
            int[] keepIndices = createComplementIndices(table.rows(), rowIndices);

            // Get column names
            List<String> columnNames = getColumnNames(table);

            if (keepIndices.length == 0) {
                // All rows are removed, create empty table
                List<Vector> emptyColumns = new ArrayList<>();
                for (int i = 0; i < table.columns(); i++) {
                    if (table.getColumn(i).getDataType().getValue() >= Entity.DATA_TYPE.DT_BOOL_ARRAY.getValue()) {
                        int scale = -1;
                        if (table.getColumn(i).getDataType() == Entity.DATA_TYPE.DT_DECIMAL32_ARRAY
                                || table.getColumn(i).getDataType() == Entity.DATA_TYPE.DT_DECIMAL64_ARRAY
                                || table.getColumn(i).getDataType() == Entity.DATA_TYPE.DT_DECIMAL128_ARRAY) {
                            scale = ((BasicArrayVector) table.getColumn(i)).getScale();
                        }
                        emptyColumns.add(new BasicArrayVector(table.getColumn(i).getDataType(), 0, scale));
                    } else {
                        int scale = -1;
                        if (table.getColumn(i).getDataType() == Entity.DATA_TYPE.DT_DECIMAL32) {
                            scale = ((BasicDecimal32Vector) table.getColumn(i)).getScale();
                        } else if (table.getColumn(i).getDataType() == Entity.DATA_TYPE.DT_DECIMAL64) {
                            scale = ((BasicDecimal64Vector) table.getColumn(i)).getScale();
                        } else if (table.getColumn(i).getDataType() == Entity.DATA_TYPE.DT_DECIMAL128) {
                            scale = ((BasicDecimal128Vector) table.getColumn(i)).getScale();
                        }
                        emptyColumns.add(BasicEntityFactory.instance().createVectorWithDefaultValue(table.getColumn(i).getDataType(), 0, scale));
                    }
                }
                return new BasicTable(columnNames, emptyColumns);
            } else {
                // There are rows to keep, create subtable (preserving original order)
                List<Vector> newColumns = new ArrayList<>();
                for (int i = 0; i < table.columns(); i++) {
                    newColumns.add(table.getColumn(i).getSubVector(keepIndices));
                }
                return new BasicTable(columnNames, newColumns);
            }
        } catch (Exception e) {
            log.error("Error in removeRows: " + e.getMessage(), e);
            return table; // Return original table when error occurs
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
            log.debug("=== UPDATE ROWS DEBUG ===");
            log.debug("Table rows: " + table.rows());
            log.debug("Row indices count: " + rowIndices.rows());
            log.debug("Update values length: " + values.length);

            // If table is empty and it's an update operation, convert to append
            if (table.rows() == 0 && rowIndices.rows() > 0) {
                log.debug("Table is empty, converting update to append");
                return appendColumns(table, values);
            }

            // For each row to update
            for (int i = 0; i < rowIndices.rows(); i++) {
                int rowIndex = ((BasicInt)rowIndices.get(i)).getInt();

                // Ensure row index is within valid range
                if (rowIndex < 0 || rowIndex >= table.rows()) {
                    log.debug("Row index " + rowIndex + " out of bounds, skipping");
                    continue;
                }

                log.debug("Updating row " + rowIndex);

                // Update each column in the row
                for (int colIndex = 0; colIndex < Math.min(values.length, table.columns()); colIndex++) {
                    try {
                        Entity value;
                        if (table.getColumn(colIndex) instanceof BasicArrayVector) {
                            value = values[colIndex];
                        } else {
                            value = values[colIndex].get(i);
                        }
                        Vector tableColumn = table.getColumn(colIndex);

                        log.debug("  Updating column " + colIndex + " (type: " + tableColumn.getDataType() + ")");
                        log.debug("    Old value: " + (tableColumn.rows() > rowIndex ? tableColumn.get(rowIndex).getString() : "N/A"));
                        log.debug("    New value: " + value.getString());

                        // Check if it's a BasicArrayVector
                        if (tableColumn instanceof BasicArrayVector) {
                            log.debug("    Detected BasicArrayVector, using enhanced set method");
                            if (!(value instanceof Vector)) {
                                System.err.println("    Error: BasicArrayVector requires Vector value, got: " + value.getClass().getSimpleName());
                                continue;
                            }
                        }

                        // Execute update
                        tableColumn.set(rowIndex, value);

                        // Verify if update was successful
                        Entity updatedValue = tableColumn.get(rowIndex);
                        log.debug("    Updated value: " + updatedValue.getString());

                        if (!updatedValue.getString().equals(value.getString())) {
                            System.err.println("    Warning: Update may not have taken effect!");
                            System.err.println("    Expected: " + value.getString());
                            System.err.println("    Actual: " + updatedValue.getString());
                        } else {
                            log.debug("    Update successful!");
                        }

                    } catch (Exception e) {
                        System.err.println("  Error updating column " + colIndex + " at row " + rowIndex + ": " + e.getMessage());
                        e.printStackTrace();
                        // Continue processing other columns, don't fail entirely because of one column
                    }
                }
            }

            log.debug("=== UPDATE ROWS COMPLETED ===");
            return true;

        } catch (Exception e) {
            System.err.println("Error in updateRows: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
}