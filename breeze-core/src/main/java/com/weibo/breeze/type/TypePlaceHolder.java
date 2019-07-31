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

import static com.weibo.breeze.type.Types.MESSAGE;

/**
 * @author zhanglei28
 * @date 2019/7/31.
 * placeholder for TypeMessage
 */
public class TypePlaceHolder<T> implements BreezeType<T> {
    private TypeMessage<T> typeMessage;
    private Class<T> clz;

    public TypePlaceHolder(Class<T> clz) {
        this.clz = clz;
    }

    @Override
    public byte getType() {
        return MESSAGE;
    }

    @Override
    public void putType(BreezeBuffer buffer) throws BreezeException {
        checkInit();
        typeMessage.putType(buffer);
    }

    @Override
    public T read(BreezeBuffer buffer, boolean withType) throws BreezeException {
        checkInit();
        return typeMessage.read(buffer, withType);
    }

    @Override
    public void write(BreezeBuffer buffer, T value, boolean withType) throws BreezeException {
        checkInit();
        typeMessage.write(buffer, value, withType);
    }

    private void checkInit() throws BreezeException {
        if (typeMessage == null) {
            typeMessage = new TypeMessage<>(clz);
        }
    }
}
