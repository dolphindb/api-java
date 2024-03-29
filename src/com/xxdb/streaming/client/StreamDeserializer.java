package com.xxdb.streaming.client;

import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.io.BigEndianDataOutputStream;
import com.xxdb.io.ExtendedDataOutput;
import com.xxdb.io.LittleEndianDataInputStream;
import org.javatuples.Pair;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamDeserializer {
    Map<String, MsgDeserializer> msgDeserializers_;
    Map<String, Pair<String, String>> tableNames_;
    public BasicMessage parse(IMessage message) throws Exception {
        if (message.size() < 3)
            throw new RuntimeException("The data must contain 3 columns. ");
        if (message.getEntity(1).getDataType() != Entity.DATA_TYPE.DT_SYMBOL && message.getEntity(1).getDataType() != Entity.DATA_TYPE.DT_STRING)
            throw new RuntimeException("The 2rd column must be a vector type with symbol or string. ");
        if (message.getEntity(2).getDataType() != Entity.DATA_TYPE.DT_BLOB)
            throw new RuntimeException("The rd column must be a vector type with blob. ");

        String sym = message.getEntity(1).getString();
        byte[] blob = ((BasicString)message.getEntity(2)).getBytes();
        MsgDeserializer deserializer = null;
        if (msgDeserializers_ != null){
            if (!msgDeserializers_.containsKey(sym))
            {
                throw new Exception("The filter " + sym + " does not exist. ");
            }else {
                deserializer = msgDeserializers_.get(sym);
            }
        }else {
            throw new RuntimeException("The StreamDeserialize is not inited");
        }
        BasicMessage mixedMessage = new BasicMessage(message.getOffset(), message.getTopic(), deserializer.parse(blob), sym);
        return mixedMessage;
    }

    public StreamDeserializer(HashMap<String, List<Entity.DATA_TYPE>> filters) {
        msgDeserializers_ = new HashMap<>();
        for(Map.Entry<String, List<Entity.DATA_TYPE>> keyValue : filters.entrySet())
        {
            List<Entity.DATA_TYPE> colTypes = keyValue.getValue();
            if (colTypes == null)
                throw new RuntimeException("The colTypes can not be null");
            msgDeserializers_.put(keyValue.getKey(), new MsgDeserializer(colTypes));
        }
    }

    public StreamDeserializer(Map<String, BasicDictionary> filters)
    {
        init(filters);
    }

    public StreamDeserializer(Map<String, Pair<String, String>> tableNames, DBConnection conn){
        this.tableNames_ = tableNames;
        if (conn != null){
            init(conn);
        }
    }

    public void init(DBConnection conn){
        if (msgDeserializers_ != null)
            throw new RuntimeException("The StreamDeserializer is inited. ");
        if (tableNames_ == null)
            throw new RuntimeException("The tableNames_ is null. ");
        msgDeserializers_ = new HashMap<>();
        Map<String, BasicDictionary> filters = new HashMap<>();
        for (Map.Entry<String, Pair<String, String>> keyValue : tableNames_.entrySet()){
            String dbName = keyValue.getValue().getValue0();
            String tableName = keyValue.getValue().getValue1();
            BasicDictionary schema = null;
            try {
                if (dbName != null){
                    if (dbName.equals("")){
                        schema = (BasicDictionary) conn.run("schema(" + tableName + ")");
                    }else {
                        schema = (BasicDictionary) conn.run("schema(loadTable(\"" + dbName + "\",\"" + tableName + "\"))");
                    }
                }
                filters.put(keyValue.getKey(), schema);
            }catch (Exception e){
                throw new RuntimeException(e.getMessage());
            }
        }
        init(filters);
    }

    public void init(Map<String, BasicDictionary> filters){
        msgDeserializers_ = new HashMap<>();
        for(Map.Entry<String, BasicDictionary> keyValue : filters.entrySet())
        {
            List<Entity.DATA_TYPE> colTypes = new ArrayList<>();
            if (keyValue.getValue() == null)
                throw new RuntimeException("The schema can not be null");
            BasicTable colDefs = (BasicTable)(keyValue.getValue()).get("colDefs");
            BasicIntVector colDefsTypeInt = (BasicIntVector)colDefs.getColumn("typeInt");
            for (int i = 0; i < colDefsTypeInt.rows(); ++i)
            {
                colTypes.add(Entity.DATA_TYPE.valueOf(colDefsTypeInt.getInt(i)));
            }
            msgDeserializers_.put(keyValue.getKey(), new MsgDeserializer(colTypes));
        }
    }

    public boolean isInited(){
        return msgDeserializers_ != null;
    }

    public void checkSchema(BasicDictionary schema) throws RuntimeException{
        BasicTable colDefs = (BasicTable) schema.get("colDefs");
        BasicStringVector types = (BasicStringVector)colDefs.getColumn(1);
        if (colDefs.rows() < 3)
            throw new RuntimeException("The data must contain 3 columns. ");
        if (!types.getString(1).equals("SYMBOL") && !types.getString(1).equals("STRING"))
            throw new RuntimeException("The 2rd column must be a vector type with symbol or string. ");
        if (!types.getString(2).equals("BLOB"))
            throw new RuntimeException("The 3rd column must be a vector type with blob. ");
    }

    class MsgDeserializer
    {
        List<Entity.DATA_TYPE> colTypes_;
        public MsgDeserializer(List<Entity.DATA_TYPE> colTypes)
        {
            colTypes_ = new ArrayList<>();
            colTypes_.addAll(colTypes);
        }

        public BasicAnyVector parse(byte[] data) throws Exception {
            ByteArrayOutputStream memoryStream = new ByteArrayOutputStream();
            ExtendedDataOutput writeStream = new BigEndianDataOutputStream(memoryStream);
            writeStream.writeBlob(data);
            LittleEndianDataInputStream dataStream = new LittleEndianDataInputStream(new ByteArrayInputStream(memoryStream.toByteArray(), 0, memoryStream.size()));
            BasicEntityFactory basicEntityFactory = (BasicEntityFactory)BasicEntityFactory.instance();

            int columns = colTypes_.size();
            dataStream.readInt();
            BasicAnyVector ret = new BasicAnyVector(columns);
//            for (int i = 0; i < columns; ++i)
//                ret.setEntity(i, basicEntityFactory.createEntity(Entity.DATA_FORM.DF_SCALAR, colTypes_.get(i), dataStream, false));
            for (int i = 0; i < columns; ++i) {
                if (colTypes_.get(i).getValue() >= Entity.DATA_TYPE.DT_BOOL_ARRAY.getValue() && colTypes_.get(i).getValue() <= Entity.DATA_TYPE.DT_DECIMAL128_ARRAY.getValue()) {
                    ret.setEntity(i, (new BasicArrayVector(colTypes_.get(i), dataStream, 1, 0, -1)).get(0));
                } else {
                    ret.setEntity(i, basicEntityFactory.createEntity(Entity.DATA_FORM.DF_SCALAR, colTypes_.get(i), dataStream, false));
                }
            }

            return ret;
        }
    }
}
