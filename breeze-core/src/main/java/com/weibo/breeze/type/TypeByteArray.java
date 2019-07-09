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

import static com.weibo.breeze.type.Types.BYTE_ARRAY;

/**
 * @author zhanglei28
 * @date 2019/7/3.
 */
public class TypeByteArray implements BreezeType<byte[]> {
    private static final byte[] EmptyByteArray = new byte[0];

    TypeByteArray() {
    }

    public static byte[] readBytes(BreezeBuffer buffer) throws BreezeException {
        int size = BreezeReader.getAndCheckSize(buffer);
        if (size == 0) {
            return EmptyByteArray;
        } else {
            byte[] b = new byte[size];
            buffer.get(b);
            return b;
        }
    }

    @Override
    public byte getType() {
        return BYTE_ARRAY;
    }

    @Override
    public byte[] read(BreezeBuffer buffer, boolean withType) throws BreezeException {
        if (withType) {
            BreezeReader.checkType(buffer, BYTE_ARRAY);
        }
        return readBytes(buffer);
    }

    @Override
    public void write(BreezeBuffer buffer, byte[] value, boolean withType) throws BreezeException {
        if (withType) {
            buffer.put(BYTE_ARRAY);
        }
        buffer.putInt(value.length);
        buffer.put(value);
    }
}
