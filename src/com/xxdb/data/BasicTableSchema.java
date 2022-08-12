package com.xxdb.data;

import com.xxdb.DBConnection;
import com.xxdb.io.ExtendedDataOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BasicTableSchema extends AbstractEntity implements Entity{
    private Map<Integer, String> name2index_ = new HashMap<>();
    private Map<Integer, Entity.DATA_TYPE> type2Index_ = new HashMap<>();
    private String tableName_;
    private DBConnection connection_;
    private int rows_;
    private int cols_;

    public BasicTableSchema(Map<Integer, Entity.DATA_TYPE> types2Index, Map<Integer, String> name2index, int rows, int cols, String tableName, DBConnection connection){
        this.type2Index_ = types2Index;
        this.name2index_ = name2index;
        this.rows_ = rows;
        this.cols_ = cols;
        this.tableName_ = tableName;
        this.connection_ = connection;
    }

    public BasicTable toBasicTable() throws IOException{
        return (BasicTable) connection_.run(tableName_);
    }

    @Override
    public Entity.DATA_FORM getDataForm() {
        return DATA_FORM.DF_TABLE;
    }

    @Override
    public DATA_CATEGORY getDataCategory() {
        return null;
    }

    @Override
    public DATA_TYPE getDataType() {
        return null;
    }

    @Override
    public int rows() {
        return rows_;
    }

    @Override
    public int columns() {
        return cols_;
    }

    @Override
    public String getString() {
        StringBuilder sb = new StringBuilder();
        sb.append("cols :" + cols_ + "\n");
        sb.append("rows :" + rows_ + "\n");
        sb.append(String.format("%-16s", "name") + String.format("%-16s", "typeString")
                + String.format("%-16s", "typeInt") + String.format("%-16s", "colIndex") + "\n");
        for (int i = 0; i < type2Index_.size(); i++){
            sb.append(String.format("%-16s", name2index_.get(i)) + String.format("%-16s", type2Index_.get(i))
                    + String.format("%-16s", type2Index_.get(i).getValue()) + String.format("%-16s", i) + "\n");
        }
        return sb.toString();
    }

    @Override
    public void write(ExtendedDataOutput output) throws IOException {

    }
}
