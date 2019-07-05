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
public class TypeFloat64 implements BreezeType<Double> {
    TypeFloat64() {
    }

    public static double readFloat64(BreezeBuffer buffer) {
        return buffer.getDouble();
    }

    @Override
    public byte getType() {
        return FLOAT64;
    }

    @Override
    public Double read(BreezeBuffer buffer, boolean withType) throws BreezeException {
        if (!withType) {
            return readFloat64(buffer);
        }
        byte type = buffer.get();
        switch (type) {
            case FLOAT64:
                return readFloat64(buffer);
            case FLOAT32:
                return (double) TypeFloat32.readFloat32(buffer);
            case STRING:
                return Double.parseDouble(TypeString.readString(buffer));
            case INT32:
                return (double) TypeInt32.readInt32(buffer);
            case INT64:
                return (double) TypeInt64.readInt64(buffer);
        }
        throw new BreezeException("Breeze cannot convert to Double. type: " + type);
    }

    @Override
    public void write(BreezeBuffer buffer, Double value, boolean withType) throws BreezeException {
        if (withType) {
            buffer.put(FLOAT64);
        }
        buffer.putDouble(value);
    }

    @Override
    public void writeMessageField(BreezeBuffer buffer, int index, Double field, boolean withType, boolean checkDefault) throws BreezeException {
        if (field != null) {
            if (checkDefault && field == 0d) {
                return;
            }
            buffer.putZigzag32(index);
            write(buffer, field, withType);
        }
    }
}
