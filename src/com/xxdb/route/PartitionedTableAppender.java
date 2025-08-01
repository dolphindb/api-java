package com.xxdb.route;

import com.xxdb.BasicDBTask;
import com.xxdb.DBConnection;
import com.xxdb.DBConnectionPool;
import com.xxdb.DBTask;
import com.xxdb.ExclusiveDBConnectionPool;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.*;

/**
 * PartitionedTableAppender is used to append a table to a partitioned table
 * across a cluster of DolphinDB instances.
 */
public class PartitionedTableAppender {
    private BasicDictionary tableInfo;
    private Domain domain;
    private int partitionColumnIdx;
    private int cols;
    private Entity.DATA_CATEGORY columnCategories[];
    private Entity.DATA_TYPE columnTypes[];
    private int threadCount;
    private DBConnectionPool pool;
    private List<ArrayList<Integer>> chunkIndices;
    private String appendScript;

	private static final Logger log = LoggerFactory.getLogger(PartitionedTableAppender.class);

    public PartitionedTableAppender(String dbUrl, String tableName, String partitionColName, DBConnectionPool pool) throws Exception {
    	this(dbUrl, tableName, partitionColName, null, pool);
    }
    
    public PartitionedTableAppender(String dbUrl, String tableName, String partitionColName, String appendFunction, DBConnectionPool pool) throws Exception {
    	this.pool = pool;
    	this.threadCount = pool.getConnectionCount();
    	chunkIndices = new ArrayList<ArrayList<Integer>>(threadCount);
    	for(int i=0; i<threadCount; ++i)
    		chunkIndices.add(new ArrayList<Integer>());
        DBConnection conn = new DBConnection();
        Entity partitionSchema;
        BasicTable colDefs;
        BasicIntVector typeInts;
        int partitionType;
        Entity.DATA_TYPE partitionColType;
        
        try {
        	DBTask task;
            if(dbUrl == null || dbUrl.isEmpty()){
            	task = new BasicDBTask("schema(" + tableName+ ")");
            	appendScript = "tableInsert{" + tableName + "}";
            }
            else{
            	task = new BasicDBTask("schema(loadTable(\"" + dbUrl + "\", \"" + tableName + "\"))");
            	appendScript = "tableInsert{loadTable('" + dbUrl + "', '" + tableName + "')}";
            }
            if(appendFunction !=null && !appendFunction.isEmpty()){
            	appendScript = appendFunction;
			}
            
            pool.execute(task);
            if(!task.isSuccessful())
            	throw new RuntimeException(task.getErrorMsg());
            tableInfo = (BasicDictionary) task.getResult();
            Entity partColNames = tableInfo.get(new BasicString("partitionColumnName"));
            if(partColNames == null)
            	throw new RuntimeException("Can't find specified partition column name.");
            if(partColNames.isScalar()){
            	if(!((BasicString)partColNames).getString().equalsIgnoreCase(partitionColName))
            		throw new RuntimeException("Can't find specified partition column name.");
            	partitionColumnIdx = ((BasicInt)tableInfo.get(new BasicString("partitionColumnIndex"))).getInt();
            	partitionSchema = tableInfo.get(new BasicString("partitionSchema"));
            	partitionType = ((BasicInt) tableInfo.get(new BasicString("partitionType"))).getInt();
            	partitionColType = Entity.DATA_TYPE.valueOf(((BasicInt) tableInfo.get(new BasicString("partitionColumnType"))).getInt());
            }
            else{
            	BasicStringVector vec = (BasicStringVector)partColNames;
            	int dims = vec.rows();
            	int index = -1;
            	for(int i=0; i<dims; ++i){
            		if(vec.getString(i).equalsIgnoreCase(partitionColName)){
            			index = i;
            			break;
            		}
            	}
            	if(index < 0)
            		throw new RuntimeException("Can't find specified partition column name.");
            	partitionColumnIdx = ((BasicIntVector)tableInfo.get(new BasicString("partitionColumnIndex"))).getInt(index);
            	partitionSchema = ((BasicAnyVector) tableInfo.get(new BasicString("partitionSchema"))).getEntity(index);
            	partitionType = ((BasicIntVector) tableInfo.get(new BasicString("partitionType"))).getInt(index);
            	partitionColType = Entity.DATA_TYPE.valueOf(((BasicIntVector) tableInfo.get(new BasicString("partitionColumnType"))).getInt(index));
            }

            colDefs = ((BasicTable) tableInfo.get(new BasicString("colDefs")));
            this.cols = colDefs.getColumn(0).rows();
            typeInts = (BasicIntVector) colDefs.getColumn("typeInt");
            this.columnCategories = new Entity.DATA_CATEGORY[this.cols];
            this.columnTypes = new Entity.DATA_TYPE[this.cols];
            for (int i = 0; i < cols; ++i) {
                this.columnTypes[i] = Entity.DATA_TYPE.valueOf(typeInts.getInt(i));
                this.columnCategories[i] = Entity.typeToCategory(this.columnTypes[i]);
            }
            domain = DomainFactory.createDomain(Entity.PARTITION_TYPE.values()[partitionType], partitionColType, partitionSchema);
        } catch (IOException e) {
            throw e;
        } finally {
            conn.close();
        }
    }
   
    public int append(Table table) throws IOException {
    	if(cols != table.columns())
    		throw new RuntimeException("The input table doesn't match the schema of the target table.");
    	for(int i=0; i<cols; ++i){
    		Vector curCol = table.getColumn(i);
    		checkColumnType(i, curCol.getDataCategory(), curCol.getDataType());
    	}
    	for(int i=0; i<threadCount; ++i)
    		chunkIndices.get(i).clear();
    	List<Integer> keys = domain.getPartitionKeys(table.getColumn(partitionColumnIdx));
    	int rows = keys.size();
    	for(int i=0; i<rows; ++i){
    		int key = keys.get(i);
    		if(key >= 0)
    			chunkIndices.get(key % threadCount).add(i);
    	}
    	List<DBTask> tasks = new ArrayList<DBTask>(threadCount);
    	for(int i=0; i<threadCount; ++i){
    		ArrayList<Integer> chunk = chunkIndices.get(i);
    		if(chunk.isEmpty())
    			continue;
    		int count = chunk.size();
    		int[] array = new int[count];
    		for(int j=0; j<count; ++j)
    			array[j] = chunk.get(j);
    		Table subTable = table.getSubTable(array);
    		ArrayList<Entity> args = new ArrayList<Entity>(1);
    		args.add(subTable);
    		tasks.add(new BasicDBTask(appendScript, args));
    	}
    	pool.execute(tasks);
    	int affected = 0;
    	for(int i=0; i<tasks.size(); ++i){
    		DBTask task = tasks.get(i);
			while (!task.isFinished()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}

    		if (task.isSuccessful()) {
    			Entity re = task.getResult();
    			if (re.getDataType() == Entity.DATA_TYPE.DT_VOID) {
					affected = 0;
				} else {
					affected += ((BasicInt)task.getResult()).getInt();
				}
			} else {
				throw new RuntimeException("Task [" + i + "] come across execption: " + task.getErrorMsg());
			}
    	}

    	return affected;
    }
    
    private void checkColumnType(int col, Entity.DATA_CATEGORY category, Entity.DATA_TYPE type) {
        Entity.DATA_CATEGORY expectCategory = this.columnCategories[col];
        Entity.DATA_TYPE expectType = this.columnTypes[col];
        if (category != expectCategory && expectCategory != Entity.DATA_CATEGORY.MIXED) {
            throw new RuntimeException("column " + col + ", expect category " + expectCategory.name() + ", got category " + category.name());
        } else if (category == Entity.DATA_CATEGORY.TEMPORAL && type != expectType) {
            throw new RuntimeException("column " + col + ", temporal column must have exactly the same type, expect " + expectType.name() + ", got " + type.name() );
        }
    }
}
