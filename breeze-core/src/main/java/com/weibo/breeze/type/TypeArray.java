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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.weibo.breeze.BreezeReader.readObjectByType;
import static com.weibo.breeze.BreezeWriter.checkWriteCount;
import static com.weibo.breeze.BreezeWriter.writeObject;
import static com.weibo.breeze.type.Types.*;

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
        if (withType) {
            byte type = buffer.get();
            if (type == NULL) {
                return null;
            }
            if (type == PACKED_ARRAY) {
                return new TypePackedArray().read(buffer, false);
            }
            if (type != ARRAY) {
                throw new BreezeException("unsupported by TypeArray. type:" + type);
            }
        }
        int size = (int) buffer.getVarint();
        if (size > Breeze.MAX_ELEM_SIZE) {
            throw new BreezeException("breeze array size over limit. size" + size);
        }
        List list = new ArrayList(size);
        readBySize(buffer, list, vType, size);
        return list;
    }

    // read collection elements by size
    @SuppressWarnings("unchecked")
    public <T> void readBySize(BreezeBuffer buffer, Collection<T> collection, Type vType, int size) throws BreezeException {
        if (size == 0) {
            return;
        }
        for (int i = 0; i < size; i++) {
            collection.add((T) readObjectByType(buffer, vType));
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
        int size = value.size();
        buffer.putVarint(size);
        if (size == 0) {
            return;
        }
        for (Object v : value) {
            writeObject(buffer, v);
        }
    }

    public void writeArray(BreezeBuffer buffer, Object[] value, boolean withType) throws BreezeException {
        checkWriteCount(buffer, value);
        if (withType) {
            buffer.put(ARRAY);
        }
        buffer.putVarint(value.length);
        if (value.length == 0) {
            return;
        }
        for (Object v : value) {
            writeObject(buffer, v);
        }
    }

}
