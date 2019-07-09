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
public class TypeInt16 implements BreezeType<Short> {
    TypeInt16() {
    }

    public static short readInt16(BreezeBuffer buffer) {
        return buffer.getShort();
    }

    @Override
    public byte getType() {
        return INT16;
    }

    @Override
    public Short read(BreezeBuffer buffer, boolean withType) throws BreezeException {
        if (!withType) {
            return readInt16(buffer);
        }
        byte type = buffer.get();
        switch (type) {
            case INT16:
                return readInt16(buffer);
            case INT32:
                return (short) TypeInt32.readInt32(buffer);
            case STRING:
                return Short.parseShort(TypeString.readString(buffer));
            case INT64:
                return (short) TypeInt64.readInt64(buffer);
        }
        throw new BreezeException("Breeze cannot convert to Short. type: " + type);
    }

    @Override
    public void write(BreezeBuffer buffer, Short value, boolean withType) throws BreezeException {
        if (withType) {
            buffer.put(INT16);
        }
        buffer.putShort(value);
    }

    @Override
    public void writeMessageField(BreezeBuffer buffer, int index, Short field, boolean withType, boolean checkDefault) throws BreezeException {
        if (field != null) {
            if (checkDefault && field == 0) {
                return;
            }
            buffer.putZigzag32(index);
            write(buffer, field, withType);
        }
    }
}
