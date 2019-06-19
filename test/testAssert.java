import org.junit.Assert;
import org.junit.Test;

public class testAssert {


    @Test
    public void t1 () {
     int a = 1;
     int b = 3;
     int c = 1;
     Assert.assertEquals(a, b);
    }

    @Test
    public void t2 () {

        Assert.assertArrayEquals(new int[]{1,2,3}, new int[] {1,2,3});
    }

}
