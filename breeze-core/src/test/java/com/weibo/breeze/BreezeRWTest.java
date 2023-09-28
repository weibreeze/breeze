package com.weibo.breeze;

import com.weibo.breeze.message.GenericMessage;
import com.weibo.breeze.message.Schema;
import com.weibo.breeze.serializer.CommonSerializer;
import com.weibo.breeze.serializer.Serializer;
import com.weibo.breeze.test.message.MyEnum;
import com.weibo.breeze.test.message.TestMsg;
import com.weibo.breeze.test.message.TestSubMsg;
import com.weibo.breeze.test.obj.TestEnum;
import com.weibo.breeze.test.obj.TestObj;
import com.weibo.breeze.test.obj.TestSubObj;
import com.weibo.breeze.test.serializer.TestObjSerializer;
import com.weibo.breeze.test.serializer.TestSubObjSerializer;
import com.weibo.breeze.type.Types;
import org.junit.Test;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Created by zhanglei28 on 2019/3/29.
 */
@SuppressWarnings("all")
public class BreezeRWTest {
    protected static <T> T testSerialize(Object object, Class<T> clz) throws BreezeException {
        BreezeBuffer buffer = new BreezeBuffer(256);
        BreezeWriter.writeObject(buffer, object);
        buffer.flip();
        byte[] result = buffer.getBytes();
        BreezeBuffer newBuffer = new BreezeBuffer(result);
        return (T) BreezeReader.readObject(newBuffer, clz);
    }

    public static TestMsg getDefaultTestMsg() {
        TestMsg testMsg = new TestMsg();
        testMsg.setMyString("testmmm");
        testMsg.setMyInt(3);
        Map<String, TestSubMsg> map = new HashMap<>();
        TestSubMsg testSubMsg = new TestSubMsg();
        testSubMsg.setMyInt(11);
        testSubMsg.setMyString("tsmmmmm");
        testSubMsg.setMyBool(true);
        testSubMsg.setMyByte(Types.MESSAGE);
        testSubMsg.setMyFloat64(23.456d);
        testSubMsg.setMyFloat32(3.1415f);
        testSubMsg.setMyBytes("xxxx".getBytes());
        testSubMsg.setMyInt64(33l);
        List<Integer> list = new ArrayList<>();
        list.add(23);
        list.add(56);
        testSubMsg.setMyArray(list);
        Map<String, byte[]> submap = new HashMap<>();
        submap.put("k1", "vv".getBytes());
        submap.put("k2", "vvvv".getBytes());
        testSubMsg.setMyMap1(submap);
        Map<Integer, List<Integer>> map2 = new HashMap<>();
        List<Integer> listx = new ArrayList();
        listx.add(234);
        listx.add(567);
        listx.add(789);
        map2.put(6, listx);
        testSubMsg.setMyMap2(map2);


        TestSubMsg testSubMsg2 = new TestSubMsg();
        testSubMsg2.setMyArray(new ArrayList<>());

        TestSubMsg testSubMsg3 = new TestSubMsg();
        testSubMsg3.setMyInt(234);

        map.put("1", testSubMsg);
        map.put("2", testSubMsg2);

        testMsg.setMyMap(map);
        testMsg.setSubMsg(testSubMsg3);

        testMsg.setMyEnum(MyEnum.E2);

        List<MyEnum> myEnums = new ArrayList<>();
        myEnums.add(MyEnum.E3);
        myEnums.add(MyEnum.E2);
        myEnums.add(MyEnum.E1);
        testMsg.setEnumArray(myEnums);
        return testMsg;
    }

    public static TestObj getDefaultTestObj() {
        TestObj testObj = new TestObj();
        testObj.setString("mytest");
        testObj.setInteger(40);
        testObj.setIntArray(new int[]{23, 33, 43, 53});
        testObj.setStringArray(new String[]{"aaa", "bbb", "ccc", "ddd"});

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
        testObj.setSubObj(tso1);
        testObj.setObjArray(new TestSubObj[]{tso1, tso2});
        return testObj;
    }

    @Test
    public void testBase() throws Exception {
        Object[][] objects = new Object[][]{
                new Object[]{true, false, false, true},
                new Object[]{"jklejroie", "873420983", "oiueeeeeeeeeeeeejjjjjjjjjjjjio2e3nlkjiofjeoiwuoejroiweurwoeijrwoeiruwejrwoierjoweroiwu389f"},
                new Object[]{(short) 12, (short) -17, Short.MAX_VALUE, Short.MIN_VALUE},
                new Object[]{1223, -3467, 12, -15, -16, 46, Integer.MAX_VALUE, Integer.MIN_VALUE},
                new Object[]{122343l, -7898l, -7l, -8l, 14l, 15l, Long.MAX_VALUE, Long.MIN_VALUE},
                new Object[]{122.343f, -78.98f, Float.MAX_VALUE, Float.MIN_VALUE},
                new Object[]{12342.343d, -34578.98d, Double.MAX_VALUE, Double.MIN_VALUE},
                new Object[]{(byte) 'x', (byte) 28, Byte.MAX_VALUE, Byte.MIN_VALUE},
                new Object[]{'x', 'd'},
                new Object[]{"x89798df".getBytes(), "usiodjfe".getBytes()},
                new Object[]{new String[]{"sdjfkljf", "n,mnzcv", "erueoiwr"}, new Integer[]{12, -13, 45, 7654, 5675}, new long[]{234l, -8l, 15l, 564l, 546435l}},
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
        assertArrayEquals(testMsg.getMyMap().get("1").getMyMap1().get("k2"), testMsg.getMyMap().get("1").getMyMap1().get("k2"));

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
    public void testGeneric() throws BreezeException {
        // --- GenericMessage to enum class ---
        // use class name
        GenericMessage enumMessage = new GenericMessage("com.weibo.breeze.test.message.MyEnum"); // only support class name by default. If want to support alias, you need to register serializer before serialization.
        enumMessage.putFields(1, 2);
        MyEnum myEnum = (MyEnum) testSerialize(enumMessage, Object.class);
        assertEquals(MyEnum.E2, myEnum);

        // use alias
        enumMessage.setName("motan.MyEnum"); // The serializer has been registered before, so using aliases can be supported
        myEnum = (MyEnum) testSerialize(enumMessage, Object.class);
        assertEquals(MyEnum.E2, myEnum);

        // specify unserialize class
        enumMessage.setName("unknown"); // The name of GenericMessage is not used when specifying the decoding type.
        myEnum = testSerialize(enumMessage, MyEnum.class);
        assertEquals(MyEnum.E2, myEnum);

        // --- GenericMessage to breeze Message class ---
        // use class name
        GenericMessage breezeMessage = new GenericMessage(TestSubMsg.class.getName());
        String s = "sub message";
        int i = 18;
        float f = 3.14f;
        breezeMessage.putFields(1, s);
        breezeMessage.putFields(2, i);
        breezeMessage.putFields(4, f);
        Map<Integer, List<Integer>> map = new HashMap<>();
        List<Integer> list = new ArrayList();
        list.add(222);
        list.add(333);
        list.add(444);
        map.put(128, list);
        breezeMessage.putFields(9, map);

        TestSubMsg testSubMsg = (TestSubMsg) testSerialize(breezeMessage, Object.class);
        assertEquals(s, testSubMsg.getMyString());
        assertEquals(i, testSubMsg.getMyInt());
        assertEquals(f, testSubMsg.getMyFloat32(), 0.01);
        assertEquals(3, testSubMsg.getMyMap2().get(128).size());

        // use alias. alias can only be used after Message is initialized.
        breezeMessage.setName("motan.TestSubMsg");
        testSubMsg = (TestSubMsg) testSerialize(breezeMessage, Object.class);
        assertEquals(f, testSubMsg.getMyFloat32(), 0.01);
        assertEquals(3, testSubMsg.getMyMap2().get(128).size());

        // specify unserialize class
        testSubMsg = (TestSubMsg) testSerialize(breezeMessage, TestSubMsg.class);
        assertEquals(f, testSubMsg.getMyFloat32(), 0.01);
        assertEquals(3, testSubMsg.getMyMap2().get(128).size());

        // complex message
        GenericMessage complexBreezeMessage = new GenericMessage(TestMsg.class.getName());
        complexBreezeMessage.putFields(1, 28);
        complexBreezeMessage.putFields(2, "complex message");
        Map<String, Object> myMap = new HashMap<>();
        myMap.put("xxx", breezeMessage);
        complexBreezeMessage.putFields(3, myMap);
        List<Object> myArray = new ArrayList<>();
        myArray.add(breezeMessage);
        complexBreezeMessage.putFields(4, myArray);
        complexBreezeMessage.putFields(5, breezeMessage);
        complexBreezeMessage.putFields(6, enumMessage);
        List<Object> enumArray = new ArrayList<>();
        enumArray.add(enumMessage);
        complexBreezeMessage.putFields(7, enumArray);

        TestMsg testMsg = (TestMsg) testSerialize(complexBreezeMessage, Object.class);
        assertEquals(28, testMsg.getMyInt());
        assertEquals("complex message", testMsg.getMyString());
        assertTrue(444 == testMsg.getMyMap().get("xxx").getMyMap2().get(128).get(2));
        assertTrue(444 == testMsg.getMyArray().get(0).getMyMap2().get(128).get(2));
        assertTrue(444 == testMsg.getSubMsg().getMyMap2().get(128).get(2));
        assertEquals(MyEnum.E2, testMsg.getMyEnum());
        assertEquals(MyEnum.E2, testMsg.getEnumArray().get(0));

        // --- GenericMessage to common class ---
        // use CommonSerializer
        Breeze.getSerializerFactory().removeSerializer(TestObj.class.getName());
        Breeze.getSerializerFactory().removeSerializer(TestSubObj.class.getName());
        GenericMessage genericMessage = new GenericMessage(TestObj.class.getName());
        genericMessage.putFields(CommonSerializer.getHash("string"), "test string");
        genericMessage.putFields(CommonSerializer.getHash("integer"), 12);
        GenericMessage subGenericMessage = new GenericMessage(TestSubObj.class.getName());
        Map<String, String> subMap = new HashMap<>();
        subMap.put("ttt", "yyy");
        subGenericMessage.putFields(CommonSerializer.getHash("map"), subMap);
        genericMessage.putFields(CommonSerializer.getHash("subObj"), subGenericMessage);
        List<Object> subList = new ArrayList<>();
        subList.add(subGenericMessage);
        genericMessage.putFields(CommonSerializer.getHash("list"), subList);
        List<Integer> integerList = new ArrayList<>();
        integerList.add(234);
        integerList.add(456);
        List<String> stringList = new ArrayList<>();
        stringList.add("aaa");
        stringList.add("bbb");
        stringList.add("ccc");
        genericMessage.putFields(CommonSerializer.getHash("intArray"), integerList);
        genericMessage.putFields(CommonSerializer.getHash("stringArray"), stringList);
        genericMessage.putFields(CommonSerializer.getHash("objArray"), subList);
        TestObj testObj = (TestObj) testSerialize(genericMessage, Object.class);
        assertEquals("test string", testObj.getString());
        assertTrue(12 == testObj.getInteger());
        assertEquals("yyy", testObj.getSubObj().getMap().get("ttt"));
        assertEquals("yyy", testObj.getList().get(0).getMap().get("ttt"));
        assertEquals(456, testObj.getIntArray()[1]);
        assertEquals("ccc", testObj.getStringArray()[2]);
        assertEquals("yyy", testObj.getObjArray()[0].getMap().get("ttt"));


        // use custom serializer
        Breeze.registerSerializer(new TestSubObjSerializer()); // need to register TestSubObjSerializer first
        Breeze.registerSerializer(new TestObjSerializer());
        GenericMessage genericMessage2 = new GenericMessage(TestObj.class.getName());
        genericMessage2.putFields(2, "test string");
        genericMessage2.putFields(3, 12);
        GenericMessage subGenericMessage2 = new GenericMessage(TestSubObj.class.getName());
        subGenericMessage2.putFields(3, subMap);
        genericMessage2.putFields(1, subGenericMessage2);
        List<Object> subList2 = new ArrayList<>();
        subList2.add(subGenericMessage2);
        genericMessage2.putFields(4, subList2);
        genericMessage2.putFields(5, integerList);
        genericMessage2.putFields(6, stringList);
        genericMessage2.putFields(7, subList2);
        testObj = (TestObj) testSerialize(genericMessage2, Object.class);
        assertEquals("test string", testObj.getString());
        assertTrue(12 == testObj.getInteger());
        assertEquals("yyy", testObj.getSubObj().getMap().get("ttt"));
        assertEquals("yyy", testObj.getList().get(0).getMap().get("ttt"));
        assertEquals(456, testObj.getIntArray()[1]);
        assertEquals("ccc", testObj.getStringArray()[2]);
        assertEquals("yyy", testObj.getObjArray()[0].getMap().get("ttt"));
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
        Breeze.getSerializerFactory().removeSerializer(TestEnum.class.getName());
        assertNull(Breeze.getSerializer(TestEnum.class.getName()));
        TestEnum testEnum = testSerialize(TestEnum.THREE, TestEnum.class);
        assertEquals(testEnum.THREE, testEnum);
        assertNotNull(Breeze.getSerializer(TestEnum.class.getName()));


        // test custom serializer
        Breeze.registerSerializer(new TestSubObjSerializer());
        Breeze.registerSerializer(new TestObjSerializer());

        serializer = Breeze.getSerializer(TestObj.class.getName());
        assertTrue(serializer instanceof TestObjSerializer);

        testObj2 = testSerialize(testObj, TestObj.class);
        assertEquals(testObj, testObj2);
    }

    @Test
    public void testCommonSerializerCompatibility() throws BreezeException {
        CommonSerializer commonSerializer = new CommonSerializer(TestSubObj.class);
        TestSubObj testSubObj = getDefaultTestObj().getSubObj();
        BreezeBuffer buffer = new BreezeBuffer(1024);
        commonSerializer.writeToBuf(testSubObj, buffer);
        buffer.flip();
        byte[] bytes = buffer.getBytes();

        BreezeBuffer buffer1 = new BreezeBuffer(bytes);
        Schema schema = new Schema();
        schema.setName(TestSubObj.class.getName())
                .putField(1, "anInt")
                .putField(2, "string")
                .putField(3, "map");
        CommonSerializer<TestSubObj> commonSerializer1 = new CommonSerializer(schema);
        TestSubObj result = commonSerializer1.readFromBuf(buffer1);
        assertEquals(testSubObj, result);
    }

    @Test
    public void testRegisterSchema() throws BreezeException {
        Schema testObjSchema = Schema.newSchema(TestObj.class.getName())
                .putField(1, "subObj")
                .putField(2, "integer")
                .putField(3, "string")
                .putField(4, "list")
                .putField(5, "intArray")
                .putField(6, "stringArray")
                .putField(7, "objArray");
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

    @Test
    public void testConcurrent() throws InterruptedException {
        // use commonSerializer
        Breeze.getSerializerFactory().removeSerializer(TestObj.class.getName());
        Breeze.getSerializerFactory().removeSerializer(TestSubObj.class.getName());

        int concurrentNum = 50;
        CyclicBarrier cyclicBarrier = new CyclicBarrier(concurrentNum);
        AtomicInteger successCount = new AtomicInteger(0);

        // concurrent serialize
        TestObj testObj = getDefaultTestObj();
        for (int i = 0; i < concurrentNum; i++) {
            new Thread(() -> {
                TestObj result = null;
                try {
                    cyclicBarrier.await();
                    result = testSerialize(testObj, TestObj.class);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                assertEquals(testObj, result);
                successCount.incrementAndGet();
            }).start();
        }
        Thread.sleep(300);
        assertEquals(concurrentNum, successCount.get());
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
                byte[] bytes2 = (byte[]) BreezeReader.readObject(newBuf, byte[].class);
                assertArrayEquals(bytes1, bytes2);
            } else if (o.getClass().isArray()) {
                if (o.getClass().getComponentType().isPrimitive()) {
                    Object ro = BreezeReader.readObject(newBuf, o.getClass());
                    for (int i = 0; i < Array.getLength(o); i++) {
                        assertEquals(Array.get(o, i), Array.get(ro, i));
                    }
                } else {
                    assertArrayEquals((Object[]) o, (Object[]) BreezeReader.readObject(newBuf, o.getClass()));
                }
            } else {
                assertEquals(o, BreezeReader.readObject(newBuf, o.getClass()));
            }
        }
    }

}