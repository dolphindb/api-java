import com.xxdb.DBConnection;
import com.xxdb.data.BasicDoubleMatrix;
import com.xxdb.data.BasicIntMatrix;
import com.xxdb.data.Entity;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Test {

    public static void testMatrixUpload() throws IOException {
        DBConnection conn = new DBConnection();
        conn.connect("192.168.1.38", 18848, "admin", "123456");
        Entity a = conn.run("cross(+, 1..5, 1..5)");
        Entity b = conn.run("1..25$5:5");
        Map<String, Entity> map = new HashMap<String, Entity>();
        map.put("a", a);
        map.put("b", b);
        conn.upload(map);
        Entity matrix = conn.run("a+b");
        assertEquals(5, matrix.rows());
        assertEquals(5, matrix.columns());
        assertTrue(((BasicIntMatrix) matrix).get(0, 0).getString().equals("3"));

        Entity matrixDoubleCross = conn.run("cross(pow,2.1 5.0 4.88,1.0 9.6 5.2)");
        Entity matrixDouble = conn.run("1..9$3:3");
        map.put("matrixDoubleCross", matrixDoubleCross);
        map.put("matrixDouble", matrixDouble);
        conn.upload(map);
        Entity matrixDoubleRes = conn.run("matrixDoubleCross + matrixDouble");
        assertEquals(3, matrixDoubleRes.rows());
        assertEquals(3, matrixDoubleRes.columns());
        assertTrue(((BasicDoubleMatrix) matrixDoubleRes).get(0, 0).getString().equals("3.1"));

        Entity matrixFloatCross = conn.run("cross(pow,2.1f 5.0f 4.88f,1.0f 9.6f 5.2f)");
        Entity matrixFloat = conn.run("take(2.33f,9)$3:3");
        map.put("matrixFloatCross", matrixFloatCross);
        map.put("matrixFloat", matrixFloat);
        conn.upload(map);
        Entity matrixFloatRes = conn.run("matrixFloatCross + matrixFloat");
        assertEquals(3, matrixFloatRes.rows());
        assertEquals(3, matrixFloatRes.columns());
        System.out.println(((BasicDoubleMatrix) matrixFloatRes).get(0, 0).getString());
        System.out.println(((BasicDoubleMatrix) matrixFloatRes).get(0, 0).getString());
        assertTrue(((BasicDoubleMatrix) matrixFloatRes).get(0, 0).getString().equals("4.43"));
    }

    public static void main(String[] args) throws IOException {
        testMatrixUpload();
    }
}
