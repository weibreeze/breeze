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

import static com.weibo.breeze.BreezeReader.*;
import static com.weibo.breeze.BreezeWriter.checkWriteCount;
import static com.weibo.breeze.type.Types.*;

/**
 * @author zhanglei28
 * @date 2019/7/2.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class TypePackedMap implements BreezeType<Map<?, ?>> {
    private BreezeType keyType; // for write
    private BreezeType valueType; // for write
    private Type kType;
    private Type vType;

    public TypePackedMap() {
    }

    public TypePackedMap(Type kType, Type vType) throws BreezeException {
        if (kType == null || vType == null) {
            throw new BreezeException("key type add value type must not null in TypePackedMap");
        }
        this.kType = kType;
        this.vType = vType;
        keyType = Breeze.getBreezeType(kType);
        valueType = Breeze.getBreezeType(vType);
        if (keyType == null || valueType == null) {
            throw new BreezeException("key type add value type must not null in TypePackedMap");
        }
    }

    @Override
    public byte getType() {
        return PACKED_MAP;
    }

    //  only used for message or serializer, key type and value type should not null.
    @Override
    public Map<?, ?> read(BreezeBuffer buffer, boolean withType) throws BreezeException {
        return read(buffer, null, kType, vType, withType);
    }

    // used for read object, key type maybe null.
    public <T, K> Map<T, K> read(BreezeBuffer buffer, Map<T, K> map, Type kType, Type vType, boolean withType) throws BreezeException {
        byte type = PACKED_MAP;
        if (withType) {
            type = buffer.get();
            if (type == NULL) {
                return null;
            }
            if (type != PACKED_MAP && type != MAP) {
                throw new BreezeException("unsupported by TypePackedMap. type:" + type);
            }
        }
        int size = (int) buffer.getVarint();
        if (map == null) {
            map = new HashMap(TypeMap.calculateInitSize(size));
        }
        if (size == 0) {
            return map;
        }
        if (type == PACKED_MAP) {
            if (keyType == null) {
                keyType = readBreezeType(buffer, kType);
                valueType = readBreezeType(buffer, vType);
            } else {
                skipType(buffer); // need check?
                skipType(buffer);
            }
            for (int i = 0; i < size; i++) {
                map.put((T) keyType.read(buffer, false), (K) valueType.read(buffer, false));
            }
        } else { // compatible with normal map.
            if (keyType != null) {
                for (int i = 0; i < size; i++) {
                    map.put((T) keyType.read(buffer), (K) valueType.read(buffer));
                }
            } else {
                for (int i = 0; i < size; i++) {
                    map.put((T) readObjectByType(buffer, kType), (K) readObjectByType(buffer, vType));
                }
            }
        }
        return map;
    }

    @Override
    public void write(BreezeBuffer buffer, Map<?, ?> value, boolean withType) throws BreezeException {
        checkWriteCount(buffer, value);
        if (withType) {
            buffer.put(PACKED_MAP);
        }
        int size = value.size();
        buffer.putVarint(size);
        if (size == 0) {
            return;
        }
        if (keyType != null) {
            keyType.putType(buffer);
            valueType.putType(buffer);
        }
        for (Map.Entry<?, ?> entry : value.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                throw new BreezeException("not support null value in breeze packed map. key:" + entry.getKey() + ", value:" + entry.getValue());
            }
            if (keyType == null) {
                keyType = Breeze.getBreezeTypeByObject(entry.getKey());
                valueType = Breeze.getBreezeTypeByObject(entry.getValue());
                keyType.putType(buffer);
                valueType.putType(buffer);
            }
            keyType.write(buffer, entry.getKey(), false);
            valueType.write(buffer, entry.getValue(), false);
        }
    }

}
