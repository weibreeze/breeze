package com.weibo.breeze;

import sun.nio.cs.ArrayDecoder;

import java.nio.charset.Charset;

/**
 * Created by zhanglei28 on 2019/6/25.
 */
public class Test {
    private static Charset charset = Charset.forName("UTF-8");
    private static double maxCharsPerByte = charset.newDecoder().maxCharsPerByte();
    private static ArrayDecoder decoder = (ArrayDecoder) charset.newDecoder();
    public static void main(String[] args) throws Exception {
        test();
        System.exit(0);
    }

    public static void test() throws Exception {
        String str = "sjdoifuwojemofindofvihj809zxje0i3jiOPU)(#JPOIj09(";
        int len = Utf8.encodedLength(str);
        int size = 500000;
        long start;
        byte[] bytes = str.getBytes("UTF-8");
        for (int j = 0; j < 10; j++) {
            start = System.currentTimeMillis();
            for (int i = 0; i < size; i++) {
//                str.getBytes("UTF-8");
//            str.getBytes(charset);
//            byte[] temp = new byte[len];
//            Utf8.encode(str, temp, 0, len);

                new String(bytes, "UTF-8");
//                decode(bytes);
//                new String(bytes, charset);
//                Utf8.decodeUtf8(bytes, 0, bytes.length);


            }
            System.out.println("time" + j + " :" + (System.currentTimeMillis() - start));
        }

    }

    public static void decode(byte[] bytes){
        char[] chars = new char[(int)(bytes.length * maxCharsPerByte)];
        int len = decoder.decode(bytes, 0, bytes.length, chars);
        if (len == chars.length){
            new String(chars);
        } else {
            new String(chars, 0, len);
        }
    }

}

