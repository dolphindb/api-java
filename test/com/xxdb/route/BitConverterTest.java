package com.xxdb.route;

import org.junit.Test;

public class BitConverterTest {
    @Test
    public void test_BitConverter_getBytes_bool(){
        boolean b1 = false;
        byte[] a = BitConverter.getBytes(b1);
        for (int i = 0; i < a.length; i++){
            System.out.println(a[i]);
        }
        boolean b2 = true;
        a = BitConverter.getBytes(b2);
        for (int i = 0; i < a.length; i++){
            System.out.println(a[i]);
        }
    }

    @Test
    public void test_BitConverter_getBytes_short(){
        short b1 = 1000;
        byte[] a = BitConverter.getBytes(b1);
        for (int i = 0; i < a.length; i++){
            System.out.println(a[i]);
        }
    }

    @Test
    public void test_BitConverter_getBytes_char(){
        char b1 = 'a';
        byte[] a = BitConverter.getBytes(b1);
        for (int i = 0; i < a.length; i++){
            System.out.println(a[i]);
        }
    }

    @Test
    public void test_BitConverter_getBytes_int(){
        int b1 = 1000;
        byte[] a = BitConverter.getBytes(b1);
        for (int i = 0; i < a.length; i++){
            System.out.println(a[i]);
        }
    }

    @Test
    public void test_BitConverter_getBytes_long(){
        long b1 = 1000;
        byte[] a = BitConverter.getBytes(b1);
        for (int i = 0; i < a.length; i++){
            System.out.println(a[i]);
        }
    }

    @Test
    public void test_BitConverter_getBytes_float(){
        float b1 = 1.1f;
        byte[] a = BitConverter.getBytes(b1);
        for (int i = 0; i < a.length; i++){
            System.out.println(a[i]);
        }
    }

    @Test
    public void test_BitConverter_getBytes_double(){
        double b1 = 1.1d;
        byte[] a = BitConverter.getBytes(b1);
        for (int i = 0; i < a.length; i++){
            System.out.println(a[i]);
        }
    }

    @Test
    public void test_BitConverter_getBytes_String(){
        String b1 = "abcdefg";
        byte[] a = BitConverter.getBytes(b1);
        for (int i = 0; i < a.length; i++){
            System.out.println(a[i]);
        }
    }

    @Test
    public void test_BitConverter_getBytes_String_charSetName(){
        String b1 = "abcdefg";
        byte[] a = BitConverter.getBytes(b1, "UTF-16");
        for (int i = 0; i < a.length; i++){
            System.out.println(a[i]);
        }
    }

    @Test
    public void test_BitConverter_toBoolean(){
        byte[] a = new byte[]{0, 1, 0};
        boolean b = BitConverter.toBoolean(a);
        System.out.println(b);
    }

    @Test
    public void test_BitConverter_toBoolean_startIndex(){
        byte[] a = new byte[]{0, 1, 0};
        boolean b = BitConverter.toBoolean(a, 1);
        System.out.println(b);
    }

    @Test
    public void test_BitConverter_toShort(){
        byte[] a = new byte[]{15, 16, 17, 40};
        short b = BitConverter.toShort(a);
        System.out.println(b);
    }

    @Test
    public void test_BitConverter_toShort_startIndex(){
        byte[] a = new byte[]{15, 16, 17, 4};
        short b = BitConverter.toShort(a, 2);
        System.out.println(b);
    }

    @Test
    public void test_BitConverter_toChar(){
        byte[] a = new byte[]{15, 16, 17, 40};
        char b = BitConverter.toChar(a);
        System.out.println(b);
    }

    @Test
    public void test_BitConverter_toChar_startIndex(){
        byte[] a = new byte[]{15, 16, 17, 40};
        char b = BitConverter.toChar(a, 2);
        System.out.println(b);
    }

    @Test
    public void test_BitConverter_toInt(){
        byte[] a = new byte[]{15, 16, 17, 40, 18, 'x', 16, 'a'};
        int b = BitConverter.toInt(a);
        System.out.println(b);
    }

    @Test
    public void test_BitConverter_toInt_startIndex(){
        byte[] a = new byte[]{15, 16, 17, 40, 18, 'x', 16, 'a'};
        int b = BitConverter.toInt(a, 4);
        System.out.println(b);
    }

    @Test
    public void test_BitConverter_toLong(){
        byte[] a = new byte[]{15, 0, 6, 40, 18, 'c', 16, 'b'};
        long b = BitConverter.toLong(a);
        System.out.println(b);
    }

    @Test
    public void test_BitConverter_toLong_startIndex(){
        byte[] a = new byte[]{15, 0, 6, 40, 18, 'c', 16, 'b', 6, 8, 3, 1, 11, 'r', 16, 's'};
        long b = BitConverter.toLong(a, 3);
        System.out.println(b);
    }

    @Test
    public void test_BitConverter_toFloat(){
        byte[] a = new byte[]{15, 0, 6, 40, 18, 'c', 16, 'b'};
        float b = BitConverter.toFloat(a);
        System.out.println(b);
    }

    @Test
    public void test_BitConverter_toFloat_startIndex(){
        byte[] a = new byte[]{15, 0, 6, 40, 18, 'c', 16, 'b'};
        float b = BitConverter.toFloat(a, 3);
        System.out.println(b);
    }

    @Test
    public void test_BitConverter_toDouble(){
        byte[] a = new byte[]{15, 0, 6, 40, 18, 'c', 16, 'b'};
        double b = BitConverter.toDouble(a);
        System.out.println(b);
    }

    @Test
    public void test_BitConverter_toDouble_startIndex(){
        byte[] a = new byte[]{15, 0, 6, 40, 18, 'c', 16, 'b'};
        double b = BitConverter.toDouble(a, 3);
        System.out.println(b);
    }

    @Test
    public void test_BitConverter_toString(){
        byte[] a = new byte[]{15, 0, 6, 40, 18, 'c', 16, 'b'};
        String b = BitConverter.toString(a);
        System.out.println(b);
    }

    @Test
    public void test_BitConverter_toString_startIndex(){
        byte[] a = new byte[]{15, 0, 6, 40, 18, 'c', 16, 'b'};
        String b = BitConverter.toString(a, "UTF-16");
        System.out.println(b);
    }

    @Test
    public void test_BitConverter_toHexString(){
        byte[] a1 = new byte[]{15, 0, 6, 40, 18, 'c', 16, 'b'};
        String b1 = BitConverter.toHexString(a1);
        System.out.println(b1);

        byte[] a2 = new byte[]{};
        String b2 = BitConverter.toHexString(a2);
        System.out.println(b2);
    }
}
