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
public class TypeInt64 implements BreezeType<Long> {
    TypeInt64() {
    }

    public static long readInt64(BreezeBuffer buffer) {
        return buffer.getZigzag64();
    }

    @Override
    public byte getType() {
        return INT64;
    }

    @Override
    public Long read(BreezeBuffer buffer, boolean withType) throws BreezeException {
        if (!withType) {
            return readInt64(buffer);
        }
        byte type = buffer.get();
        // direct int64
        if (type <= DIRECT_INT64_MAX_TYPE) {
            return (long) (type - INT64_ZERO);
        }
        // compatible for some types
        switch (type) {
            case INT64:
                return readInt64(buffer);
            case INT32:
                return (long) TypeInt32.readInt32(buffer);
            case STRING:
                return Long.parseLong(TypeString.readString(buffer));
            case INT16:
                return (long) TypeInt16.readInt16(buffer);
        }
        throw new BreezeException("Breeze cannot convert to Long. type: " + type);
    }

    @Override
    public void write(BreezeBuffer buffer, Long value, boolean withType) throws BreezeException {
        if (withType) {
            // direct int64
            if (value >= DIRECT_INT64_MIN_VALUE && value <= DIRECT_INT64_MAX_VALUE) {
                buffer.put((byte) (value + INT64_ZERO));
                return;
            }
            buffer.put(INT64);
        }
        buffer.putZigzag64(value);
    }

    @Override
    public void writeMessageField(BreezeBuffer buffer, int index, Long field, boolean withType, boolean checkDefault) throws BreezeException {
        if (field != null) {
            if (checkDefault && field == 0) {
                return;
            }
            buffer.putVarint(index);
            write(buffer, field, withType);
        }
    }
}
