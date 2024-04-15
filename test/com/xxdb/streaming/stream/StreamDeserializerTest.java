package com.xxdb.streaming.stream;
import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.streaming.client.BasicMessage;
import com.xxdb.streaming.client.IMessage;
import com.xxdb.streaming.client.StreamDeserializer;
import junit.framework.Assert;
import org.javatuples.Pair;
import org.junit.Test;

import java.sql.Blob;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.xxdb.BehaviorTest.bundle;

public class StreamDeserializerTest {
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    @Test(expected = RuntimeException.class)
    public void test_StreamDeserializer_notnull(){
        HashMap<String, List<Entity.DATA_TYPE>> filters = new HashMap<>();
        List<Entity.DATA_TYPE> list = new ArrayList<>();
        list.add(Entity.DATA_TYPE.DT_STRING);
        list.add(Entity.DATA_TYPE.DT_SYMBOL);
        filters.put("String",list);
        StreamDeserializer sd = new StreamDeserializer(filters);
        sd.init(new DBConnection());
    }

    @Test(expected = RuntimeException.class)
    public void test_StreamDeserializer_lt3(){
        HashMap<String, List<Entity.DATA_TYPE>> filters = new HashMap<>();
        List<Entity.DATA_TYPE> list = new ArrayList<>();
        list.add(Entity.DATA_TYPE.DT_STRING);
        list.add(Entity.DATA_TYPE.DT_SYMBOL);
        filters.put("String",list);
        StreamDeserializer sd = new StreamDeserializer(filters);
        BasicDictionary bd = new BasicDictionary(Entity.DATA_TYPE.DT_STRING, Entity.DATA_TYPE.DT_DICTIONARY,1);
        List<String> colNames = new ArrayList<>();
        colNames.add("cbool");
        colNames.add("cchar");
        List<Vector> cols = new ArrayList<Vector>(){};


        //boolean
        byte[] vbool = new byte[]{1,0};
        BasicBooleanVector bbv = new BasicBooleanVector(vbool);
        cols.add(bbv);
        //char

        String[] vchar = new String[]{"Java","Python"};
        BasicStringVector bcv = new BasicStringVector(vchar);
        cols.add(bcv);
        BasicTable bt = new BasicTable(colNames,cols);
        bd.put(new BasicString("colDefs"),bt);
        sd.checkSchema(bd);
    }

    @Test(expected = RuntimeException.class)
    public void test_StreamDeserializer_notSymbol(){
        HashMap<String, List<Entity.DATA_TYPE>> filters = new HashMap<>();
        List<Entity.DATA_TYPE> list = new ArrayList<>();
        list.add(Entity.DATA_TYPE.DT_STRING);
        list.add(Entity.DATA_TYPE.DT_SYMBOL);
        filters.put("String",list);
        StreamDeserializer sd = new StreamDeserializer(filters);
        BasicDictionary bd = new BasicDictionary(Entity.DATA_TYPE.DT_STRING, Entity.DATA_TYPE.DT_DICTIONARY,1);
        List<String> colNames = new ArrayList<>();
        colNames.add("cbool");
        colNames.add("cchar");
        //colNames.add("cint");
        List<Vector> cols = new ArrayList<Vector>(){};


        //boolean
        byte[] vbool = new byte[]{1,0,1};
        BasicBooleanVector bbv = new BasicBooleanVector(vbool);
        cols.add(bbv);
        //char

        String[] vchar = new String[]{"Java","Python","Go"};
        BasicStringVector bcv = new BasicStringVector(vchar);
        cols.add(bcv);

        BasicTable bt = new BasicTable(colNames,cols);
        bd.put(new BasicString("colDefs"),bt);
        sd.checkSchema(bd);
    }

    @Test
    public void test_StreamDeserializer_checkSchema(){
        HashMap<String, List<Entity.DATA_TYPE>> filters = new HashMap<>();
        List<Entity.DATA_TYPE> list = new ArrayList<>();
        list.add(Entity.DATA_TYPE.DT_STRING);
        list.add(Entity.DATA_TYPE.DT_SYMBOL);
        filters.put("String",list);
        StreamDeserializer sd = new StreamDeserializer(filters);
        BasicDictionary bd = new BasicDictionary(Entity.DATA_TYPE.DT_STRING, Entity.DATA_TYPE.DT_DICTIONARY,1);
        List<String> colNames = new ArrayList<>();
        colNames.add("cbool");
        colNames.add("cchar");
        List<Vector> cols = new ArrayList<Vector>(){};
        BasicStringVector bsv1 = new BasicStringVector(new String[]{"Java","python","go"});
        cols.add(bsv1);
        BasicStringVector bsv2 = new BasicStringVector(new String[]{"SQL","SYMBOL","NoSQL"});
        cols.add(bsv2);

        BasicTable bt = new BasicTable(colNames,cols);
        System.out.println(bt.rows());
        System.out.println(bt.getString());
        bd.put(new BasicString("colDefs"),bt);
        String re = null;
        try{
            sd.checkSchema(bd);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The 3rd column must be a vector type with blob. ",re);

        List<Vector> cols1 = new ArrayList<Vector>(){};
        BasicStringVector bsv11 = new BasicStringVector(new String[]{"Java","python"});
        cols1.add(bsv11);
        //char
        BasicStringVector bsv21 = new BasicStringVector(new String[]{"SQL","SYMBOL"});
        cols1.add(bsv21);
        BasicTable bt1 = new BasicTable(colNames,cols1);
        bd.put(new BasicString("colDefs"),bt1);
        String re1 = null;
        try{
            sd.checkSchema(bd);
        }catch(Exception ex){
            re1 = ex.getMessage();
        }
        Assert.assertEquals("The data must contain 3 columns. ",re1);

        List<Vector> cols2 = new ArrayList<Vector>(){};
        BasicStringVector bsv12 = new BasicStringVector(new String[]{"Java","python","cdd"});
        cols2.add(bsv12);
        //char
        BasicStringVector bsv22 = new BasicStringVector(new String[]{"INT","SQL","BLOB"});
        cols2.add(bsv22);
        BasicTable bt2 = new BasicTable(colNames,cols2);
        bd.put(new BasicString("colDefs"),bt2);
        String re2 = null;
        try{
            sd.checkSchema(bd);
        }catch(Exception ex){
            re2 = ex.getMessage();
        }
        Assert.assertEquals("The 2rd column must be a vector type with symbol or string. ",re2);
        List<Vector> cols3 = new ArrayList<Vector>(){};
        BasicStringVector bsv13 = new BasicStringVector(new String[]{"Java","python","cdd"});
        cols3.add(bsv13);
        //char
        BasicStringVector bsv23 = new BasicStringVector(new String[]{"INT","STRING","BLOB"});
        cols3.add(bsv23);
        BasicTable bt3 = new BasicTable(colNames,cols3);
        bd.put(new BasicString("colDefs"),bt3);
        sd.checkSchema(bd);

    }

    @Test
    public void test_StreamDeserializer_parse() throws Exception {
        HashMap<String,Integer> map = new HashMap<>();
        map.put("dolphindb",1);
        map.put("mongodb",2);
        map.put("gaussdb",3);
        map.put("goldendb",4);
        BasicAnyVector bav = new BasicAnyVector(4);
        bav.set(0,new BasicInt(5));
        bav.set(1,new BasicPoint(6.4,9.2));
        bav.set(2,new BasicString("China"));
        bav.set(3,new BasicDouble(15.48));
        System.out.println(bav.getString());
        IMessage message =new BasicMessage(0L,"first",bav,map);
        System.out.println(message.getEntity(2).getDataType());
        HashMap<String, List<Entity.DATA_TYPE>> filters = new HashMap<>();
        List<Entity.DATA_TYPE> list = new ArrayList<>();
        list.add(Entity.DATA_TYPE.DT_STRING);
        list.add(Entity.DATA_TYPE.DT_SYMBOL);
        filters.put("String",list);
        StreamDeserializer sd = new StreamDeserializer(filters);
        String re = null;
        try{
            sd.parse(message);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The 2rd column must be a vector type with symbol or string. ",re);
    }

    @Test
    public void test_StreamDeserializer_messageNull() throws Exception {
        HashMap<String,Integer> map = new HashMap<>();
        map.put("dolphindb",1);
        map.put("mongodb",2);
        map.put("gaussdb",3);
        map.put("goldendb",4);
        BasicAnyVector bav = new BasicAnyVector(4);
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT);
        Entity res = conn.run("blob(\"hello\")");
        System.out.println(res.getDataType());
        bav.set(0,new BasicInt(5));
        bav.set(1,new BasicString("DataBase"));
        bav.set(2, (Scalar) res);
        bav.set(3,new BasicDouble(15.48));
        System.out.println(bav.getString());
        IMessage message =new BasicMessage(0L,"first",bav,map);
        Pair<String,String> table = new Pair<>("DataBase","Dolphindb");
        HashMap<String,Pair<String,String>> tableNames = new HashMap<>();
        tableNames.put("test",table);
        StreamDeserializer sd = new StreamDeserializer(tableNames,null);
        String re = null;
        try{
            sd.parse(message);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("The StreamDeserialize is not inited",re);
    }

}
