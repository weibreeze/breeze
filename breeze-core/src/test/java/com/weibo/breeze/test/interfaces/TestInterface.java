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

package com.weibo.breeze.test.interfaces;

import com.weibo.breeze.message.Schema;
import com.weibo.breeze.test.message.MyEnum;
import com.weibo.breeze.test.message.TestSubMsg;
import com.weibo.breeze.test.obj.TestEnum;
import com.weibo.breeze.test.obj.TestObj;
import com.weibo.breeze.test.obj.TestSubObj;

import java.util.List;
import java.util.Map;

/**
 * @author zhanglei28
 * @date 2023/9/14.
 */
public interface TestInterface {

    List<TestSubObj> createList(Map<String, List<Integer>> param1, TestSubMsg param2);

    Map<String, List<Schema>> createMap(TestEnum param1, MyEnum param2, Map<String, List<Map<Integer, TestObj>>> param3);

    void noParams();
}
