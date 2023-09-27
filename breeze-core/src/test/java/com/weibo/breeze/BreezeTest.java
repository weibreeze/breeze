/*
 *
 *   Copyright 2023 Weibo, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.weibo.breeze;

import com.weibo.breeze.serializer.CommonSerializer;
import com.weibo.breeze.serializer.Serializer;
import com.weibo.breeze.serializer.SerializerFactory;
import com.weibo.breeze.test.message.MyEnum;
import com.weibo.breeze.test.message.TestMsg;
import com.weibo.breeze.test.obj.TestEnum;
import com.weibo.breeze.test.obj.TestObj;
import com.weibo.breeze.test.obj.TestSubObj;
import com.weibo.breeze.type.BreezeType;
import com.weibo.breeze.type.TypeMessage;
import com.weibo.breeze.type.TypePackedArray;
import com.weibo.breeze.type.TypePackedMap;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static com.weibo.breeze.type.Types.*;
import static org.junit.Assert.*;

/**
 * @author zhanglei28
 * @date 2023/9/26.
 */
@SuppressWarnings("all")
public class BreezeTest {

    @Test
    public void testGetSerializer() {
        // init
        SerializerFactory serializerFactory = Breeze.getSerializerFactory();
        serializerFactory.removeSerializer(TestObj.class.getName());
        serializerFactory.removeSerializer(TestSubObj.class.getName());
        assertNull(serializerFactory.getSerializer(TestObj.class.getName()));
        assertNull(serializerFactory.getSerializer(TestSubObj.class.getName()));

        // get by class
        Serializer serializer = serializerFactory.getSerializer(TestObj.class);
        assertTrue(serializer instanceof CommonSerializer);
        assertSame(serializer, serializerFactory.getSerializer(TestObj.class.getName())); // same object
        Serializer subSerializer = serializerFactory.getSerializer(TestSubObj.class.getName());
        assertNotNull(subSerializer);
        assertSame(subSerializer, serializerFactory.getSerializer(TestSubObj.class));
    }

    @Test
    public void testConcurrentGetSerializer() throws InterruptedException {
        SerializerFactory serializerFactory = Breeze.getSerializerFactory();
        serializerFactory.removeSerializer(TestObj.class.getName());
        serializerFactory.removeSerializer(TestSubObj.class.getName());

        int concurrentNum = 20;
        ConcurrentHashMap<String, Serializer> resultMap = new ConcurrentHashMap<>();
        CyclicBarrier cyclicBarrier = new CyclicBarrier(concurrentNum);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < concurrentNum; i++) {
            new Thread(() -> {
                try {
                    cyclicBarrier.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                Serializer serializer = serializerFactory.getSerializer(TestObj.class);
                assertNotNull(serializer);
                Serializer old = resultMap.putIfAbsent(TestObj.class.getName(), serializer);
                if (old != null) {
                    assertSame(serializer, old);
                }
                Serializer subSerializer = serializerFactory.getSerializer(TestSubObj.class);
                assertNotNull(subSerializer);
                old = resultMap.putIfAbsent(TestSubObj.class.getName(), subSerializer);
                if (old != null) {
                    assertSame(subSerializer, old);
                }
                successCount.incrementAndGet();
            }).start();
        }
        Thread.sleep(300);
        assertEquals(concurrentNum, successCount.get());
    }

    @Test
    public void testGetBreezeType() throws BreezeException {
        // get breeze type by class
        Object[][] objects = new Object[][]{
                new Object[]{String.class, TYPE_STRING},
                new Object[]{Integer.class, TYPE_INT32},
                new Object[]{Boolean.class, TYPE_BOOL},
                new Object[]{Long.class, TYPE_INT64},
                new Object[]{Short.class, TYPE_INT16},
                new Object[]{Float.class, TYPE_FLOAT32},
                new Object[]{Double.class, TYPE_FLOAT64},
                new Object[]{Byte.class, TYPE_BYTE},
                new Object[]{byte[].class, TYPE_BYTE_ARRAY},
                new Object[]{String[].class, new TypePackedArray()},
                new Object[]{ArrayList.class, new TypePackedArray()},
                new Object[]{HashSet.class, new TypePackedArray()},
                new Object[]{TestMsg.class, new TypeMessage(new TestMsg())},
                new Object[]{LinkedHashMap.class, new TypePackedMap()},
                new Object[]{TestObj.class, new TypeMessage(TestObj.class)},
                new Object[]{MyEnum.class, new TypeMessage(MyEnum.class)},
                new Object[]{TestEnum.class, new TypeMessage(TestEnum.class)},
        };

        for (Object[] object : objects) {
            checkGetBreezeTypeByClass((Class<?>) object[0], (BreezeType) object[1]);
        }
        BreezeType bt = Breeze.getBreezeType(BreezeType.class);
        assertNull(bt);
    }

    private void checkGetBreezeTypeByClass(Class<?> clazz, BreezeType breezeType) throws BreezeException {
        BreezeType bt = Breeze.getBreezeType(clazz);
        assertEquals(breezeType.getClass(), bt.getClass());
        if (breezeType instanceof TypeMessage) {
            BreezeBuffer buffer1 = new BreezeBuffer(32);
            BreezeBuffer buffer2 = new BreezeBuffer(32);
            bt.putType(buffer1);
            breezeType.putType(buffer2);
            buffer1.flip();
            buffer2.flip();
            assertArrayEquals(buffer1.getBytes(), buffer2.getBytes());
        }
    }
}
