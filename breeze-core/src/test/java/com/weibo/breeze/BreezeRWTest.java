package com.weibo.breeze;

import com.weibo.breeze.message.GenericMessage;
import com.weibo.breeze.message.Schema;
import com.weibo.breeze.serializer.CommonSerializer;
import com.weibo.breeze.serializer.Serializer;
import com.weibo.breeze.test.message.TestMsg;
import com.weibo.breeze.test.message.TestSubMsg;
import com.weibo.breeze.test.obj.TestEnum;
import com.weibo.breeze.test.obj.TestObj;
import com.weibo.breeze.test.obj.TestSubObj;
import com.weibo.breeze.test.serializer.TestObjSerializer;
import com.weibo.breeze.test.serializer.TestSubObjSerializer;
import org.junit.Test;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by zhanglei28 on 2019/3/29.
 */
@SuppressWarnings("all")
public class BreezeRWTest {

    @Test
    public void testBase() throws Exception {
        Object[][] objects = new Object[][]{
                new Object[]{true, false, false, true},
                new Object[]{"jklejroie", "873420983", "oiueeeeeeeeeeeeejjjjjjjjjjjjio2e3nlkf"},
                new Object[]{(short) 12, (short) -17, Short.MAX_VALUE, Short.MIN_VALUE},
                new Object[]{1223, -3467, Integer.MAX_VALUE, Integer.MIN_VALUE},
                new Object[]{122343l, -7898l, Long.MAX_VALUE, Long.MIN_VALUE},
                new Object[]{122.343f, -78.98f, Float.MAX_VALUE, Float.MIN_VALUE},
                new Object[]{12342.343d, -34578.98d, Double.MAX_VALUE, Double.MIN_VALUE},
                new Object[]{(byte) 'x', (byte) 28, Byte.MAX_VALUE, Byte.MIN_VALUE},
                new Object[]{'x', 'd'},
                new Object[]{"x89798df".getBytes(), "usiodjfe".getBytes()},
                new Object[]{new String[]{"sdjfkljf", "n,mnzcv","erueoiwr"}, new Integer[]{12,45,7654,5675}, new long[]{234l, 564l, 546435l}},
        };
        for (Object[] obj : objects) {
            testBase(obj);
        }
    }

    @Test
    public void testMessage() throws Exception {
        TestMsg testMsg = getDefaultTestMsg();
        TestMsg testMsg1 = testSerialize(testMsg, TestMsg.class);
        assertTrue(testMsg.equals(testMsg1));
        assertArrayEquals(testMsg.getMap().get("1").getMap().get("k2"), testMsg.getMap().get("1").getMap().get("k2"));

        // generic message
        GenericMessage genericMessage = new GenericMessage();
        genericMessage.putFields(1, "sjdjfierf");
        genericMessage.putFields(2, 234);
        genericMessage.putFields(6, true);
        List<String> list = new ArrayList<>();
        list.add("sdjfk");
        list.add("ueiruw");
        genericMessage.putFields(9, list);
        Map<Integer, Float> map = new HashMap<>();
        map.put(445, 6734.6456f);
        map.put(2133, 89453.445f);
        genericMessage.putFields(334344, map);
        GenericMessage genericMessage2 = (GenericMessage) testSerialize(genericMessage, Object.class);
        assertEquals(genericMessage.getFields(), genericMessage2.getFields());
    }

    @Test
    public void testSerializer() throws Exception {
        Breeze.getSerializerFactory().removeSerializer(TestObj.class.getName());
        //test commonserializer
        assertNull(Breeze.getSerializer(TestObj.class.getName()));
        TestObj testObj = getDefaultTestObj();
        TestObj testObj2 = testSerialize(testObj, TestObj.class);
        assertEquals(testObj, testObj2);
        assertNotNull(Breeze.getSerializer(TestObj.class.getName()));
        Serializer serializer = Breeze.getSerializer(TestObj.class.getName());
        assertTrue(serializer instanceof CommonSerializer);

        //test enum serializer
        assertNull(Breeze.getSerializer(TestEnum.class.getName()));
        TestEnum testEnum = testSerialize(TestEnum.THREE, TestEnum.class);
        assertEquals(testEnum.THREE, testEnum);
        assertNotNull(Breeze.getSerializer(TestEnum.class.getName()));


        // test custom serializer
        Breeze.registerSerializer(new TestObjSerializer());
        Breeze.registerSerializer(new TestSubObjSerializer());

        serializer = Breeze.getSerializer(TestObj.class.getName());
        assertTrue(serializer instanceof TestObjSerializer);

        testObj2 = testSerialize(testObj, TestObj.class);
        assertEquals(testObj, testObj2);
    }

    @Test
    public void testRegisterSchema() throws BreezeException {
        Schema testObjSchema = Schema.newSchema(TestObj.class.getName())
                .putField(1, "subObj")
                .putField(2, "integer")
                .putField(3, "string")
                .putField(4, "list");
        CommonSerializer commonSerializer = new CommonSerializer(testObjSchema);
        Breeze.registerSerializer(commonSerializer);

        Schema testSubObjSchema = Schema.newSchema(TestSubObj.class.getName())
                .putField(1, "anInt")
                .putField(2, "string")
                .putField(3, "map");
        Breeze.registerSerializer(new CommonSerializer(testSubObjSchema));
        assertNotNull(Breeze.getSerializer(TestObj.class.getName()));
        assertTrue(commonSerializer == Breeze.getSerializer(TestObj.class.getName()));

        TestObj testObj = getDefaultTestObj();
        TestObj testObj2 = testSerialize(testObj, TestObj.class);
        assertEquals(testObj, testObj2);
    }

    @Test(expected = BreezeException.class)
    public void testCircularReference() throws BreezeException {
        Map map = new HashMap<>();
        List list = new ArrayList();
        list.add(map);
        map.put("1", list);

        Breeze.setMaxWriteCount(0);
        assertEquals(0, Breeze.getMaxWriteCount());
        Breeze.setMaxWriteCount(3);
        assertEquals(3, Breeze.getMaxWriteCount());
        BreezeBuffer buf = new BreezeBuffer(128);
        BreezeWriter.writeObject(buf, map);
        throw new RuntimeException("should not here");
    }


    private void testBase(Object[] expects) throws BreezeException {
        BreezeBuffer buf = new BreezeBuffer(64);

        for (Object o : expects) {
            BreezeWriter.writeObject(buf, o);
        }
        buf.flip();
        BreezeBuffer newBuf = new BreezeBuffer(buf.getBytes());
        for (Object o : expects) {
            if (o.getClass() == byte[].class) {
                byte[] bytes1 = (byte[]) o;
                byte[] bytes2 = BreezeReader.readObject(newBuf, byte[].class);
                assertArrayEquals(bytes1, bytes2);
            } else if (o.getClass().isArray()){
                if (o.getClass().getComponentType().isPrimitive()){
                    Object ro = BreezeReader.readObject(newBuf, o.getClass());
                    for (int i = 0; i < Array.getLength(o); i++) {
                        assertEquals(Array.get(o, i), Array.get(ro, i));
                    }
                }else{
                    assertArrayEquals((Object[])o, (Object[])BreezeReader.readObject(newBuf, o.getClass()));
                }
            } else{
                assertEquals(o, BreezeReader.readObject(newBuf, o.getClass()));
            }
        }
    }

    protected static <T> T testSerialize(Object object, Class<T> clz) throws BreezeException {
        BreezeBuffer buffer = new BreezeBuffer(256);
        BreezeWriter.writeObject(buffer, object);
        buffer.flip();
        byte[] result = buffer.getBytes();
        BreezeBuffer newBuffer = new BreezeBuffer(result);
        return BreezeReader.readObject(newBuffer, clz);
    }


    public TestObj getDefaultTestObj() {
        TestObj testObj = new TestObj();
        testObj.setString("mytest");
        testObj.setInteger(40);

        TestSubObj tso1 = new TestSubObj();
        tso1.setAnInt(38);
        tso1.setString("subtest message");
        Map<String, String> map = new HashMap<>();
        map.put("tk1", "tv1");
        map.put("tk2", "tv2");
        tso1.setMap(map);

        TestSubObj tso2 = new TestSubObj();
        tso2.setAnInt(39);
        tso2.setString("subtest message--2");
        Map<String, String> map2 = new HashMap<>();
        map2.put("tk3", "tv3");
        map2.put("tk4", "tv4");
        tso2.setMap(map2);

        List<TestSubObj> list = new ArrayList<>();
        list.add(tso1);
        list.add(tso2);
        testObj.setList(list);
        return testObj;
    }

    public TestMsg getDefaultTestMsg() {
        TestMsg testMsg = new TestMsg();
        testMsg.setString("testmmm");
        testMsg.setAnInt(3);
        Map<String, TestSubMsg> map = new HashMap<>();
        TestSubMsg testSubMsg = new TestSubMsg();
        testSubMsg.setAnInt(11);
        testSubMsg.setString("tsmmmmm");
        testSubMsg.setaBoolean(true);
        testSubMsg.setaByte(BreezeType.MESSAGE);
        testSubMsg.setaDouble(23.456d);
        testSubMsg.setaFloat(3.1415f);
        testSubMsg.setBytes("xxxx".getBytes());
        testSubMsg.setaLong(33l);
        List<Integer> list = new ArrayList<>();
        list.add(23);
        list.add(56);
        testSubMsg.setList(list);
        Map<String, byte[]> submap = new HashMap<>();
        submap.put("k1", "vv".getBytes());
        submap.put("k2", "vvvv".getBytes());
        testSubMsg.setMap(submap);
        Map<Integer, List> map2 = new HashMap<>();
        List listx = new ArrayList();
        listx.add(234);
        listx.add("jheiur");
        listx.add(true);
        map2.put(6, listx);
        testSubMsg.setMap2(map2);

        TestSubMsg testSubMsg2 = new TestSubMsg();
        testSubMsg2.setList(new ArrayList<>());

        map.put("1", testSubMsg);
        map.put("2", testSubMsg2);

        testMsg.setMap(map);
        return testMsg;
    }

}