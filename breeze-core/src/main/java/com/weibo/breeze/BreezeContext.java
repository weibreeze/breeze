/*
 *
 *   Copyright 2019 Weibo, Inc.
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

import java.util.HashMap;
import java.util.Map;


/**
 * @author zhanglei28
 * @date 2019/6/11.
 */
public class BreezeContext {
    private Map<Integer, String> messageTypeRefName = new HashMap<>();
    private Map<String, Integer> messageTypeRefIndex = new HashMap<>();
    private int messageTypeRefCount = 0;
    private WriteCounter writeCounter;

    public String getMessageTypeName(int index) {
        return messageTypeRefName.get(index);
    }

    public Integer getMessageTypeIndex(String name) {
        return messageTypeRefIndex.get(name);
    }

    public void putMessageType(String name) {
        if (!messageTypeRefIndex.containsKey(name)) {
            int index = ++messageTypeRefCount;
            messageTypeRefIndex.put(name, index);
            messageTypeRefName.put(index, name);
        }
    }

    public int writeCount(int hash) {
        if (writeCounter == null) {
            writeCounter = new WriteCounter();
        }
        return writeCounter.put(hash);
    }

    public static class WriteCounter {
        private Map<Integer, Integer> map = new HashMap<>();

        public int put(int hash) {
            Integer count = map.get(hash);
            if (count == null) {
                count = 0;
            }
            count = count + 1;
            map.put(hash, count);
            return count;
        }
    }
}
