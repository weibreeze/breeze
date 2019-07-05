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
import com.weibo.breeze.BreezeReader;

import static com.weibo.breeze.type.Types.BYTE;

/**
 * @author zhanglei28
 * @date 2019/7/3.
 */
public class TypeByte implements BreezeType<Byte> {
    TypeByte() {
    }

    public static byte readByte(BreezeBuffer buffer) {
        return buffer.get();
    }

    @Override
    public byte getType() {
        return BYTE;
    }

    @Override
    public Byte read(BreezeBuffer buffer, boolean withType) throws BreezeException {
        if (withType) {
            BreezeReader.checkType(buffer, BYTE);
        }
        return readByte(buffer);
    }

    @Override
    public void write(BreezeBuffer buffer, Byte value, boolean withType) throws BreezeException {
        if (withType) {
            buffer.put(BYTE);
        }
        buffer.put(value);
    }
}
