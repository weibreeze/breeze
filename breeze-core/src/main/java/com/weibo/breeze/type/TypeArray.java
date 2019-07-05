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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.weibo.breeze.BreezeReader.getAndCheckSize;
import static com.weibo.breeze.BreezeReader.readObjectByType;
import static com.weibo.breeze.BreezeWriter.checkWriteCount;
import static com.weibo.breeze.BreezeWriter.writeObject;
import static com.weibo.breeze.type.Types.ARRAY;
import static com.weibo.breeze.type.Types.NULL;

/**
 * @author zhanglei28
 * @date 2019/7/2.
 */
public class TypeArray implements BreezeType<List<?>> {
    private Type vType;

    public TypeArray() {
        vType = Object.class;
    }

    public TypeArray(Type type) throws BreezeException {
        if (type == null) {
            throw new BreezeException("value type must not null in TypeArray");
        }
        this.vType = type;
    }

    @Override
    public byte getType() {
        return ARRAY;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<?> read(BreezeBuffer buffer, boolean withType) throws BreezeException {
        List list = new ArrayList();
        read(buffer, list, vType, withType);
        return list;
    }

    @SuppressWarnings("unchecked")
    public <T> void read(BreezeBuffer buffer, Collection<T> collection, Type vType, boolean withType) throws BreezeException {
        if (withType) {
            byte type = buffer.get();
            if (type == NULL) {
                return;
            }
            if (type != ARRAY) {
                throw new BreezeException("unsupported by TypeArray. type:" + type);
            }
        }
        int size = getAndCheckSize(buffer);
        if (size != 0) {
            int startPos = buffer.position();
            int endPos = startPos + size;
            while (buffer.position() < endPos) {
                collection.add((T) readObjectByType(buffer, vType));
            }
            if (buffer.position() != endPos) {
                throw new BreezeException("Breeze deserialize wrong array size, except: " + size + " actual: " + (buffer.position() - startPos));
            }
        }
    }

    @Override
    public void write(BreezeBuffer buffer, List<?> value, boolean withType) throws BreezeException {
        writeCollection(buffer, value, withType);
    }

    public void writeCollection(BreezeBuffer buffer, Collection<?> value, boolean withType) throws BreezeException {
        checkWriteCount(buffer, value);
        if (withType) {
            buffer.put(ARRAY);
        }
        if (value.isEmpty()) {
            buffer.putInt(0);
            return;
        }
        int pos = buffer.position();
        buffer.position(pos + 4);
        for (Object v : value) {
            writeObject(buffer, v);
        }
        int newPos = buffer.position();
        buffer.position(pos);
        buffer.putInt(newPos - pos - 4);
        buffer.position(newPos);
    }

    public void writeArray(BreezeBuffer buffer, Object[] value, boolean withType) throws BreezeException {
        checkWriteCount(buffer, value);
        if (withType) {
            buffer.put(ARRAY);
        }
        if (value.length == 0) {
            buffer.putInt(0);
            return;
        }
        int pos = buffer.position();
        buffer.position(pos + 4);
        for (Object v : value) {
            writeObject(buffer, v);
        }
        int newPos = buffer.position();
        buffer.position(pos);
        buffer.putInt(newPos - pos - 4);
        buffer.position(newPos);
    }

}
