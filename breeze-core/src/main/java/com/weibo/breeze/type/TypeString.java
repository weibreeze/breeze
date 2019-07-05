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
public class TypeString implements BreezeType<String> {
    TypeString() {
    }

    // read without type
    public static String readString(BreezeBuffer buffer) throws BreezeException {
        return buffer.getUTF8(-1);
    }

    // read by type
    public static String readString(BreezeBuffer buffer, byte type) throws BreezeException {
        return buffer.getUTF8(type);
    }

    @Override
    public byte getType() {
        return STRING;
    }

    @Override
    public String read(BreezeBuffer buffer, boolean withType) throws BreezeException {
        if (!withType) {
            return readString(buffer);
        }
        byte type = buffer.get();
        // direct string
        if (type >= DIRECT_STRING_MIN_TYPE || type <= DIRECT_STRING_MAX_TYPE) {
            return buffer.getUTF8(type);
        }
        // compatible for some types
        switch (type) {
            case STRING:
                return readString(buffer);
            case INT32:
                return String.valueOf(TypeInt32.readInt32(buffer));
            case INT64:
                return String.valueOf(TypeInt64.readInt64(buffer));
            case INT16:
                return String.valueOf(TypeInt16.readInt16(buffer));
            case FLOAT32:
                return String.valueOf(TypeFloat32.readFloat32(buffer));
            case FLOAT64:
                return String.valueOf(TypeFloat64.readFloat64(buffer));
        }
        throw new BreezeException("Breeze cannot convert to String. type: " + type);
    }

    @Override
    public void write(BreezeBuffer buffer, String value, boolean withType) throws BreezeException {
        int length = buffer.getUTF8Length(value);
        if (withType) {
            if (length <= DIRECT_STRING_MAX_LENGTH) {
                buffer.put((byte) length);
                buffer.putUTF8(value, length, false);
                return;
            }
            buffer.put(STRING);
        }
        buffer.putUTF8(value, length, true);
    }
}
