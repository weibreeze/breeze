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

package com.weibo.breeze.protobuf;

import com.google.protobuf.ByteString;
import com.weibo.breeze.BreezeBuffer;
import com.weibo.breeze.BreezeException;
import com.weibo.breeze.serializer.Serializer;
import com.weibo.breeze.type.Types;

public class ByteStringSerializer implements Serializer<ByteString> {
    private final String[] names;

    public ByteStringSerializer(Class<?> clz) {
        names = new String[]{clz.getName()};
    }

    @Override
    public void writeToBuf(ByteString obj, BreezeBuffer buffer) throws BreezeException {
        Types.TYPE_BYTE_ARRAY.write(buffer, obj.toByteArray());

    }

    @Override
    public ByteString readFromBuf(BreezeBuffer buffer) throws BreezeException {
        return ByteString.copyFrom(Types.TYPE_BYTE_ARRAY.read(buffer));
    }

    @Override
    public String[] getNames() {
        return names;
    }
}
