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

package com.weibo.breeze.serializer;

import com.weibo.breeze.BreezeBuffer;
import com.weibo.breeze.BreezeException;
import com.weibo.breeze.BreezeReader;
import com.weibo.breeze.BreezeWriter;

import java.math.BigInteger;

import static com.weibo.breeze.type.Types.TYPE_STRING;

/**
 * @author zhanglei28
 * @date 2019/6/4.
 */
public class BigIntegerSerializer implements Serializer<BigInteger> {
    private static final String[] names = new String[]{BigInteger.class.getName()};

    @Override
    public void writeToBuf(BigInteger obj, BreezeBuffer buffer) throws BreezeException {
        BreezeWriter.writeMessage(buffer, () -> TYPE_STRING.writeMessageField(buffer, 1, obj.toString()));
    }

    @Override
    public BigInteger readFromBuf(BreezeBuffer buffer) throws BreezeException {
        BigInteger[] objects = new BigInteger[1];
        BreezeReader.readMessage(buffer, (int index) -> {
            switch (index) {
                case 1:
                    objects[0] = new BigInteger(TYPE_STRING.read(buffer));
                    break;
            }
        });
        return objects[0];
    }

    @Override
    public String[] getNames() {
        return names;
    }
}
