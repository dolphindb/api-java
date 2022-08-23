import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.streaming.client.StreamDeserializer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class StreamDeserializerTest {
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

//        int[] vint = new int[]{3,4,5};
//        BasicIntVector biv = new BasicIntVector(vint);
//        cols.add(biv);
        BasicTable bt = new BasicTable(colNames,cols);
        bd.put(new BasicString("colDefs"),bt);
        sd.checkSchema(bd);
    }

    @Test(expected = RuntimeException.class)
    public void test_StreamDeserializer_notVector(){
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
        BasicStringVector bsv = new BasicStringVector(new String[]{"Java","python","go"});
        cols.add(bsv);
        //char

        BasicStringVector bsv2 = new BasicStringVector(new String[]{"SQL","SYMBOL","NoSQL"});
        cols.add(bsv2);

//        int[] vint = new int[]{3,4,5};
//        BasicIntVector biv = new BasicIntVector(vint);
//        cols.add(biv);
        BasicTable bt = new BasicTable(colNames,cols);
        bd.put(new BasicString("colDefs"),bt);
        sd.checkSchema(bd);

    }

}
