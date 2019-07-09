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
public class TypeFloat32 implements BreezeType<Float> {
    TypeFloat32() {
    }

    public static float readFloat32(BreezeBuffer buffer) {
        return buffer.getFloat();
    }

    @Override
    public byte getType() {
        return FLOAT32;
    }

    @Override
    public Float read(BreezeBuffer buffer, boolean withType) throws BreezeException {
        if (!withType) {
            return readFloat32(buffer);
        }
        byte type = buffer.get();
        switch (type) {
            case FLOAT32:
                return readFloat32(buffer);
            case FLOAT64:
                return (float) TypeFloat64.readFloat64(buffer);
            case STRING:
                return Float.parseFloat(TypeString.readString(buffer));
            case INT32:
                return (float) TypeInt32.readInt32(buffer);
            case INT64:
                return (float) TypeInt64.readInt64(buffer);
        }
        throw new BreezeException("Breeze cannot convert to Float. type: " + type);
    }

    @Override
    public void write(BreezeBuffer buffer, Float value, boolean withType) throws BreezeException {
        if (withType) {
            buffer.put(FLOAT32);
        }
        buffer.putFloat(value);
    }

    @Override
    public void writeMessageField(BreezeBuffer buffer, int index, Float field, boolean withType, boolean checkDefault) throws BreezeException {
        if (field != null) {
            if (checkDefault && field == 0f) {
                return;
            }
            buffer.putZigzag32(index);
            write(buffer, field, withType);
        }
    }
}
