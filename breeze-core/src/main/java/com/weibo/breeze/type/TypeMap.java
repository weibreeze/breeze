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

package com.weibo.breeze.type;

import com.weibo.breeze.Breeze;
import com.weibo.breeze.BreezeBuffer;
import com.weibo.breeze.BreezeException;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import static com.weibo.breeze.BreezeReader.readObjectByType;
import static com.weibo.breeze.BreezeWriter.checkWriteCount;
import static com.weibo.breeze.BreezeWriter.writeObject;
import static com.weibo.breeze.type.Types.*;

/**
 * @author zhanglei28
 * @date 2019/7/2.
 */
public class TypeMap implements BreezeType<Map<?, ?>> {
    private Type kType;
    private Type vType;

    public TypeMap() {
        kType = Object.class;
        vType = Object.class;
    }

    public TypeMap(Type kType, Type vType) throws BreezeException {
        if (kType == null || vType == null) {
            throw new BreezeException("key type add value type must not null in TypeMap");
        }
        this.kType = kType;
        this.vType = vType;
    }

    public static int calculateInitSize(int size) throws BreezeException {
        if (size <= 10) {
            return 16;
        }
        if (size > Breeze.MAX_ELEM_SIZE) {
            throw new BreezeException("breeze map size over limit. size" + size);
        }
        return (int) (size / 0.75) + 1;
    }

    @Override
    public byte getType() {
        return MAP;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map read(BreezeBuffer buffer, boolean withType) throws BreezeException {
        return read(buffer, null, kType, vType, withType);
    }

    @SuppressWarnings("unchecked")
    public <T, K> Map<T, K> read(BreezeBuffer buffer, Map<T, K> map, Type kType, Type vType, boolean withType) throws BreezeException {
        if (withType) {
            byte type = buffer.get();
            if (type == NULL) {
                return null;
            }
            if (type == PACKED_MAP) {
                return new TypePackedMap().read(buffer, map, kType, vType, false);
            }
            if (type != MAP) {
                throw new BreezeException("unsupported by TypeMap. type:" + type);
            }
        }
        int size = (int) buffer.getVarint();
        if (map == null) {
            map = new HashMap(calculateInitSize(size));
        }
        if (size == 0) {
            return map;
        }
        for (int i = 0; i < size; i++) {
            map.put((T) readObjectByType(buffer, kType), (K) readObjectByType(buffer, vType));
        }
        return map;
    }

    @Override
    public void write(BreezeBuffer buffer, Map<?, ?> value, boolean withType) throws BreezeException {
        checkWriteCount(buffer, value);
        if (withType) {
            buffer.put(MAP);
        }
        int size = value.size();
        buffer.putVarint(size);
        if (size == 0) {
            return;
        }
        for (Map.Entry<?, ?> entry : value.entrySet()) {
            writeObject(buffer, entry.getKey());
            writeObject(buffer, entry.getValue());
        }
    }
}
