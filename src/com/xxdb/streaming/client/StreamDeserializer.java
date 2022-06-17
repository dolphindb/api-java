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
            throw new Exception("The data must contain 3 columns. ");
        if (message.getEntity(1).getDataType() != Entity.DATA_TYPE.DT_SYMBOL && message.getEntity(1).getDataType() != Entity.DATA_TYPE.DT_STRING)
            throw new Exception("The 2rd column must be a vector type with symbol or string. ");
        if (message.getEntity(2).getDataType() != Entity.DATA_TYPE.DT_BLOB)
            throw new Exception("The rd column must be a vector type with blob. ");

        String sym = message.getEntity(1).getString();
        byte[] blob = ((BasicString)message.getEntity(2)).getBytes();
        MsgDeserializer deserializer = null;
        if (!msgDeserializers_.containsKey(sym))
        {
            throw new Exception("The filter " + sym + " does not exist. ");
        }else {
            deserializer = msgDeserializers_.get(sym);
        }
        BasicMessage mixedMessage = new BasicMessage(message.getOffset(), message.getTopic(), deserializer.parse(blob), sym);
        return mixedMessage;
    }

    public StreamDeserializer(HashMap<String, List<Entity.DATA_TYPE>> filters) {
        msgDeserializers_ = new HashMap<>();
        for(Map.Entry<String, List<Entity.DATA_TYPE>> keyValue : filters.entrySet())
        {
            List<Entity.DATA_TYPE> colTypes = keyValue.getValue();
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
            BasicDictionary schema;
            try {
                if (tableName == ""){
                    schema = (BasicDictionary) conn.run("schema(" + dbName + ")");
                }else {
                    schema = (BasicDictionary) conn.run("schema(loadTable(\"" + dbName + "\",\"" + tableName + "\"))");
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
            List<String> colNames = new ArrayList<>();

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

    class MsgDeserializer
    {
        List<Entity.DATA_TYPE> colTypes_;
        public MsgDeserializer(List<Entity.DATA_TYPE> colTypes)
        {
            colTypes_ = new ArrayList<>();
            colTypes_.addAll(colTypes);
        }

        public BasicAnyVector parse(byte[] data) throws IOException {
            ByteArrayOutputStream memoryStream = new ByteArrayOutputStream();
            ExtendedDataOutput writeStream = new BigEndianDataOutputStream(memoryStream);
            writeStream.writeBlob(data);
            LittleEndianDataInputStream dataStream = new LittleEndianDataInputStream(new ByteArrayInputStream(memoryStream.toByteArray(), 0, memoryStream.size()));
            BasicEntityFactory basicEntityFactory = (BasicEntityFactory)BasicEntityFactory.instance();

            int columns = colTypes_.size();
            dataStream.readInt();
            BasicAnyVector ret = new BasicAnyVector(columns);
            for (int i = 0; i < columns; ++i)
                ret.setEntity(i, basicEntityFactory.createEntity(Entity.DATA_FORM.DF_SCALAR, colTypes_.get(i), dataStream, false));
            return ret;
        }
    }
}
