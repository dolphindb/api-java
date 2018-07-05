# DolphinDB Java API

#### 1. Requirement

DolphinDB Java API requires Java 8 and above.

#### 2. Use Java API

To use DolphinDB Java API, please add dolphindb.jar to your library path. This file is located under the folder of "bin". Otherwise, it can be compiled using the source code provided.



#### 3. Mapping between Java Objects and DolphinDB Objects


Java API adopts interface-oriented programming. Java API uses the class interface "Entity" to represent all data types returned from DolphinDB. Java API provides 7 types of extended interfaces: scalar, vector, matrix, set, dictionary, table and chart based on the "Entity" interface and DolphinDB data forms. They are included in the package of com.xxdb.data.

| Extended interfaces| Naming rules| Examples|
| :------ |:------| :-----|
| scalar      |Basic+<DataType> | BasicInt, BasicDouble, BasicDate, etc.|
| vector and matrix |Basic+<DataForm> | BasicIntVector, BasicDoubleMatrix, BasicAnyVector, etc.|
| set, dictionary and table |Basic+<DataForm>  |BasicSet, BasicDictionary, BasicTable. |
| chart | |BasicChart
       
      
"Basic" indicates the basic implementation of a data form interface, <DataType> indicates a DolphinDB data type, and <DataForm> indicates a DolphinDB data form.


#### 4. Setup DolphinDB connection


Java API connects to DolphinDB server through TCP/IP protocol. To establish a connection, specify the host and port of the DolphinDB server as illustrated by the example below.

```
import com.xxdb;

  
DBConnection conn = new DBConnection();

boolean success = conn.connect("localhost", 8848);


//Or with login info

boolean success = conn.connect("localhost", 8848, "admin", "123456");

```


#### 5. Run Scripts


You can use the following statement to run DolphinDB script. The maximum length of a script is 65,535 bytes.


```
conn.run("<SCRIPT>");
```

If the script contains a statement, it will return a data object. If the script contains multiple statements, it will return the last object that they generate. If the script contains errors or if network issues occur, it will throw an IOException.


#### 5.1 Vector



In the example below, the DolphinDB script **"rand(`IBM`MSFT`GOOG`BIDU, 10)"** returns the Java object BasicStringVector. The method vector.rows() indicates the size of the vector. To access an element in a vector, use the method vector.getString(index).

```
public void testStringVector() throws IOException{

BasicStringVector vector = (BasicStringVector)conn.run("rand(`IBM`MSFT`GOOG`BIDU, 10)");

       int size = vector.rows();

       System.out.println("size: "+size);

       for(int i=0; i<size; ++i)

               System.out.println(vector.getString(i));

}
```


Similarly, you can work with a double vector or a tuple.


```
public void testDoubleVector() throws IOException{

       BasicDoubleVector vector = (BasicDoubleVector)conn.run("rand(10.0, 10)");

       int size = vector.rows();

       System.out.println("size: "+size);

       for(int i=0; i<size; ++i)

               System.out.println(vector.getDouble(i));

}


public void testAnyVector() throws IOException{

       BasicAnyVector result = (BasicAnyVector)conn.run("[1, 2, [1,3,5],[0.9, [0.8]]]");

       System.out.println(result.getString());

}
```


#### 5.2 Set


```
public void testSet() throws IOException{

               BasicSet result = (BasicSet)conn.run("set(1+3*1..100)");

               System.out.println(result.getString());

}
```
       

#### 5.3 Matrix



To access an element from an integer matrix, use getInt(row, col). To get the number of rows or columns, use functions rows() and columns() respectively.


```
public void testIntMatrix() throws IOException {

       BasicIntMatrix matrix = (BasicIntMatrix)conn.run("1..6$3:2");

       System.out.println(matrix.getString());

}
```

#### 5.4 Dictionary


To get all keys and values from a dictionary, use functions keys() and values() respectively. To look up a value in a dictionary, use the method get(key).

```
public void testDictionary() throws IOException{

       BasicDictionary dict = (BasicDictionary)conn.run("dict(1 2 3,`IBM`MSFT`GOOG)");

       //to print the corresponding value for key 1.        

      System.out.println(dict.get(new BasicInt(1)).getString());

}
```


#### 5.5 Table



To get a table column, use method table.getColumn(index); to get column names, use method table.getColumnName(index); to get table column/row size, use table.columns()/table.rows().

```
public void testTable() throws IOException{

StringBuilder sb =new StringBuilder();

       sb.append("n=2000\n");                

      sb.append("syms=`IBM`C`MS`MSFT`JPM`ORCL\n");

       sb.append("mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms, n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price)\n");

       sb.append("select qty,price from mytrades where sym=`IBM");

       BasicTable table = (BasicTable)conn.run(sb.toString());

       System.out.println(table.getString());

}
```


#### 5.6 Null Object



To get a "NULL" object, you can execute the following script and then call method obj.getDataType()


```
public void testVoid() throws IOException{

       Entity obj = conn.run("NULL");

       System.out.println(obj.getDataType());

}
```




#### 6. Run DolphinDB Functions



We can call either a DolphinDB built-in function or a user defined function. The example below passes a double vector to the server and calls the sum function.


```
public void testFunction() throws IOException{

       List<Entity> args = new ArrayList<Entity>(1);

       BasicDoubleVector vec = new BasicDoubleVector(3);

       vec.setDouble(0, 1.5);

       vec.setDouble(1, 2.5);

       vec.setDouble(2, 7);

       args.add(vec);

       Scalar result = (Scalar)conn.run("sum", args);

       System.out.println(result.getString());

}
```




#### 7. Upload Objects to DolphinDB Server



We can upload a binary data object to a DolphinDB server and assign it to a variable for future use. The variable name can use 3 types of characters: letter, digit, and underscore. The first character must be a letter.


```
public void testFunction() throws IOException{

       List<Entity> args = new ArrayList<Entity>(1);

List<Entity> vars = new ArrayList<String>(1);

       BasicDoubleVector vec = new BasicDoubleVector(3);

       vec.setDouble(0, 1.5);

       vec.setDouble(1, 2.5);

       vec.setDouble(2, 7);

       args.add(vec);

      vars.add("a");

       conn.run(vars,args);

       Entity result = conn.run("accumulate(+,a)");

       System.out.println(result.getString());

}
```

