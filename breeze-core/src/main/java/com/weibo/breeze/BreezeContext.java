package com.weibo.breeze;

import java.util.HashMap;
import java.util.Map;


/**
 * Created by zhanglei28 on 2019/6/11.
 */
public class BreezeContext {
    private Map<Integer, String> messageTypeRefName = new HashMap<>();
    private Map<String, Integer> messageTypeRefIndex = new HashMap<>();
    private int messageTypeRefCount = 0;
    private WriteCountor writeCountor;

    public boolean withType = true; // withtype for message only

    public String getMessageTypeName(int index) {
        return messageTypeRefName.get(index);
    }

    public Integer getMessageTypeIndex(String name) {
        return messageTypeRefIndex.get(name);
    }

    public void putMessageType(String name){
        if (!messageTypeRefIndex.containsKey(name)){
            int index = ++messageTypeRefCount;
            messageTypeRefIndex.put(name, index);
            messageTypeRefName.put(index, name);
        }
    }

    public int writeCount(int hash) {
        if (writeCountor == null) {
            writeCountor = new WriteCountor();
        }
        return writeCountor.put(hash);
    }

    public static class WriteCountor {
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
