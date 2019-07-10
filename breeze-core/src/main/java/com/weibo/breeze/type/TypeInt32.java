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

import static com.weibo.breeze.type.Types.*;

/**
 * @author zhanglei28
 * @date 2019/7/2.
 */
public class TypeInt32 implements BreezeType<Integer> {
    TypeInt32() {
    }

    public static int readInt32(BreezeBuffer buffer) {
        return buffer.getZigzag32();
    }

    @Override
    public byte getType() {
        return INT32;
    }

    @Override
    public Integer read(BreezeBuffer buffer, boolean withType) throws BreezeException {
        if (!withType) {
            return readInt32(buffer);
        }
        byte type = buffer.get();
        // direct int32
        if (type >= DIRECT_INT32_MIN_TYPE && type <= DIRECT_INT32_MAX_TYPE) {
            return type - INT32_ZERO;
        }
        // compatible for some types
        switch (type) {
            case INT32:
                return readInt32(buffer);
            case STRING:
                return Integer.parseInt(TypeString.readString(buffer));
            case INT64:
                return (int) TypeInt64.readInt64(buffer);
            case INT16:
                return (int) TypeInt16.readInt16(buffer);
        }
        throw new BreezeException("Breeze cannot convert to Integer. type: " + type);
    }

    @Override
    public void write(BreezeBuffer buffer, Integer value, boolean withType) throws BreezeException {
        if (withType) {
            // direct int32
            if (value >= DIRECT_INT32_MIN_VALUE && value <= DIRECT_INT32_MAX_VALUE) {
                buffer.put((byte) (value + INT32_ZERO));
                return;
            }
            buffer.put(INT32);
        }
        buffer.putZigzag32(value);
    }

    @Override
    public void writeMessageField(BreezeBuffer buffer, int index, Integer field, boolean withType, boolean checkDefault) throws BreezeException {
        if (field != null) {
            if (checkDefault && field == 0) {
                return;
            }
            buffer.putVarint(index);
            write(buffer, field, withType);
        }
    }
}
