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

import com.weibo.breeze.BreezeBuffer;
import com.weibo.breeze.BreezeException;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import static com.weibo.breeze.BreezeReader.getAndCheckSize;
import static com.weibo.breeze.BreezeReader.readObjectByType;
import static com.weibo.breeze.BreezeWriter.checkWriteCount;
import static com.weibo.breeze.BreezeWriter.writeObject;
import static com.weibo.breeze.type.Types.MAP;
import static com.weibo.breeze.type.Types.NULL;

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

    @Override
    public byte getType() {
        return MAP;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map read(BreezeBuffer buffer, boolean withType) throws BreezeException {
        Map map = new HashMap();
        read(buffer, map, kType, vType, withType);
        return map;
    }

    @SuppressWarnings("unchecked")
    public <T, K> void read(BreezeBuffer buffer, Map<T, K> map, Type kType, Type vType, boolean withType) throws BreezeException {
        if (withType) {
            byte type = buffer.get();
            if (type == NULL) {
                return;
            }
            if (type != MAP) {
                throw new BreezeException("unsupported by TypeMap. type:" + type);
            }
        }

        int size = getAndCheckSize(buffer);
        if (size != 0) {
            int startPos = buffer.position();
            int endPos = startPos + size;
            while (buffer.position() < endPos) {
                map.put((T) readObjectByType(buffer, kType), (K) readObjectByType(buffer, vType));
            }

            if (buffer.position() != endPos) {
                throw new BreezeException("Breeze deserialize wrong map size, except: " + size + " actual: " + (buffer.position() - startPos));
            }
        }
    }

    @Override
    public void write(BreezeBuffer buffer, Map<?, ?> value, boolean withType) throws BreezeException {
        checkWriteCount(buffer, value);
        if (withType) {
            buffer.put(MAP);
        }
        if (value.isEmpty()) {
            buffer.putInt(0);
            return;
        }
        int pos = buffer.position();
        buffer.position(pos + 4);
        for (Map.Entry<?, ?> entry : value.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            writeObject(buffer, entry.getKey());
            writeObject(buffer, entry.getValue());
        }
        int newPos = buffer.position();
        buffer.position(pos);
        buffer.putInt(newPos - pos - 4);
        buffer.position(newPos);
    }

}
