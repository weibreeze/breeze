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

import static com.weibo.breeze.BreezeReader.*;
import static com.weibo.breeze.BreezeWriter.checkWriteCount;
import static com.weibo.breeze.type.Types.*;

/**
 * @author zhanglei28
 * @date 2019/7/2.
 */
public class TypePackedArray implements BreezeType<List<?>> {
    private BreezeType valueType;
    private Type vType;

    public TypePackedArray() {
    }

    public TypePackedArray(Type type) throws BreezeException {
        if (type == null) {
            throw new BreezeException("value type must not null in TypePackedArray");
        }
        this.vType = type;
        this.valueType = Breeze.getBreezeType(type);
        if (valueType == null) {
            throw new BreezeException("value type must not null in TypePackedArray");
        }
    }

    @Override
    public byte getType() {
        return PACKED_ARRAY;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<?> read(BreezeBuffer buffer, boolean withType) throws BreezeException {
        byte type = PACKED_ARRAY;
        if (withType) {
            type = buffer.get();
            if (type == NULL) {
                return null;
            }
            if (type != PACKED_ARRAY && type != ARRAY) {
                throw new BreezeException("unsupported by TypePackedArray. type:" + type);
            }
        }
        int size = (int) buffer.getVarint();
        if (size > Breeze.MAX_ELEM_SIZE) {
            throw new BreezeException("breeze array size over limit. size" + size);
        }
        List list = new ArrayList(size);
        readBySize(buffer, list, vType, size, type == PACKED_ARRAY);
        return list;
    }

    @SuppressWarnings("unchecked")
    public <T> void readBySize(BreezeBuffer buffer, Collection<T> collection, Type vType, int size, boolean isPacked) throws BreezeException {
        if (size == 0) {
            return;
        }
        if (isPacked) {
            if (valueType == null) {
                valueType = readBreezeType(buffer, vType);
            } else {
                skipType(buffer); // need check?
            }
            for (int j = 0; j < size; j++) {
                collection.add((T) valueType.read(buffer, false));
            }
        } else {
            if (valueType != null) {
                for (int j = 0; j < size; j++) {
                    collection.add((T) valueType.read(buffer));
                }
            } else {
                for (int j = 0; j < size; j++) {
                    collection.add((T) readObjectByType(buffer, vType));
                }
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(BreezeBuffer buffer, List<?> value, boolean withType) throws BreezeException {
        checkWriteCount(buffer, value);
        if (withType) {
            buffer.put(PACKED_ARRAY);
        }
        int size = value.size();
        buffer.putVarint(size);
        if (size == 0) {
            return;
        }
        if (valueType != null) {
            valueType.putType(buffer);
        }
        for (Object v : value) {
            if (v == null) {
                throw new BreezeException("not support null value in breeze packed array");
            }
            if (valueType == null) {
                valueType = Breeze.getBreezeType(v.getClass());
                valueType.putType(buffer);
            }
            valueType.write(buffer, v, false);
        }
    }
}
