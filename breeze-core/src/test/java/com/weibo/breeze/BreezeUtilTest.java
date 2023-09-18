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

import com.weibo.breeze.message.Schema;
import com.weibo.breeze.test.interfaces.TestInterface;
import com.weibo.breeze.test.obj.TestEnum;
import com.weibo.breeze.test.obj.TestObj;
import com.weibo.breeze.test.obj.TestSubObj;
import com.weibo.breeze.test.serializer.TestSubObjSerializer;
import junit.framework.TestCase;

import java.util.Map;

/**
 * @author zhanglei28
 * @date 2023/9/14.
 */
public class BreezeUtilTest extends TestCase {

    public void testGenerateSchema() throws BreezeException {
        Map<Class<?>, BreezeUtil.GenerateClassResult> resultMap;
        Breeze.getSerializerFactory().removeSerializer(TestObj.class.getName());
        Breeze.getSerializerFactory().removeSerializer(TestSubObj.class.getName());
        resultMap = BreezeUtil.generateSchema(TestObj.class);
        assertEquals(2, resultMap.size());
        assertEquals(8, resultMap.get(TestObj.class).schema.getFields().size());
        assertEquals(3, resultMap.get(TestSubObj.class).schema.getFields().size());

        // has custom serializer
        Breeze.registerSerializer(new TestSubObjSerializer()); // need to register TestSubObjSerializer first
        resultMap = BreezeUtil.generateSchema(TestObj.class);
        assertEquals(1, resultMap.size());

        // interface
        Breeze.getSerializerFactory().removeSerializer(TestObj.class.getName());
        Breeze.getSerializerFactory().removeSerializer(TestSubObj.class.getName());
        resultMap = BreezeUtil.generateSchema(TestInterface.class);
        assertEquals(4, resultMap.size());
        assertNotNull(resultMap.get(TestObj.class));
        assertNotNull(resultMap.get(TestSubObj.class));
        assertNotNull(resultMap.get(TestEnum.class));
        assertNotNull(resultMap.get(Schema.class));
    }
}