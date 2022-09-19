package com.xxdb.route;

import com.xxdb.DBConnection;
import com.xxdb.data.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AutoFitTableUpsert {
    private DBConnection connection_;
    private String upsertScript_;
    private int cols_;
    private List<Entity.DATA_CATEGORY> columnCategories_ = new ArrayList<>();
    private List<Entity.DATA_TYPE> columnTypes_ = new ArrayList<>();
    private List<String> colNames_ = new ArrayList<>();

    public AutoFitTableUpsert(String dbUrl, String tableName, DBConnection connection, boolean ignoreNull, String[] pkeyColNames, String[] psortColumns) throws IOException {
        connection_ = connection;
        BasicTable colDefs;
        BasicIntVector colTypesInt;
        BasicDictionary tableInfo;
        BasicStringVector colNames;
        try {
            String task;
            if (dbUrl.equals("")){
                task = "schema(" + tableName+ ")";
                upsertScript_ =  "upsert!{" + tableName + "";
            }else {
                task = "schema(loadTable(\"" + dbUrl + "\", \"" + tableName + "\"))";
                upsertScript_ = "upsert!{loadTable('" + dbUrl + "', '" + tableName + "')";
            }
            upsertScript_+=",";
            if(!ignoreNull)
                upsertScript_+=",ignoreNull=false";
            else
                upsertScript_+=",ignoreNull=true";
            int ignoreParamCount=0;
            if (pkeyColNames != null && pkeyColNames.length > 0){
                upsertScript_+=",keyColNames=";
                for (String one : pkeyColNames){
                    upsertScript_ += "`"+one;
                }
            }else {
                ignoreParamCount++;
            }
            if (psortColumns != null && psortColumns.length > 0){
                while (ignoreParamCount > 0){
                    upsertScript_ += ",";
                    ignoreParamCount--;
                }
                upsertScript_ += ",sortColumns=";
                for (String one : psortColumns){
                    upsertScript_ += "`"+one;
                }
            }
            upsertScript_+="}";
            tableInfo = (BasicDictionary) connection_.run(task);
            colDefs = (BasicTable) tableInfo.get(new BasicString("colDefs"));
            cols_ = colDefs.rows();
            colTypesInt = (BasicIntVector) colDefs.getColumn("typeInt");
            colNames = (BasicStringVector) colDefs.getColumn("name");
            for (int i = 0; i < cols_; i++){
                columnTypes_.add(Entity.DATA_TYPE.valueOf(colTypesInt.getInt(i)));
                columnCategories_.add(Utils.getCategory(columnTypes_.get(i)));
                colNames_.add(colNames.getString(i));
            }
        }catch (IOException e){
            throw e;
        }
    }

    public int upsert(BasicTable table) throws Exception{
        if (cols_ != table.columns())
            throw new RuntimeException("The input table columns doesn't match the columns of the target table.");
        List<Vector> colums = new ArrayList<>();
        for (int i = 0; i < cols_; i++){
            Vector curCol = table.getColumn(i);
            checkColumnType(i, curCol.getDataCategory(), curCol.getDataType());
            if (columnCategories_.get(i) == Entity.DATA_CATEGORY.TEMPORAL && curCol.getDataType() != columnTypes_.get(i)){
                colums.add((Vector) Utils.castDateTime(curCol, columnTypes_.get(i)));
            }else {
                colums.add(curCol);
            }
        }
        BasicTable tableToInsert = new BasicTable(colNames_, colums);
        List<Entity> args = new ArrayList<>();
        args.add(tableToInsert);
        Entity res = connection_.run(upsertScript_, args);
        if (res.getDataType() == Entity.DATA_TYPE.DT_INT && res.getDataForm() == Entity.DATA_FORM.DF_SCALAR){
            return ((BasicInt) res).getInt();
        }else
            return 0;
    }

    private void checkColumnType(int col, Entity.DATA_CATEGORY category, Entity.DATA_TYPE type){
        if (columnTypes_.get(col) != type){
            Entity.DATA_CATEGORY expectCateGory = columnCategories_.get(col);
            if (category != expectCateGory)
                throw new RuntimeException("column " + col + ", expect category " + expectCateGory + ", got category " + category);
        }
    }
}
