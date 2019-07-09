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

/**
 * @author zhanglei28
 * @date 2019/7/2.
 */
public interface BreezeType<T> {
    byte getType();

    T read(BreezeBuffer buffer, boolean withType) throws BreezeException;

    void write(BreezeBuffer buffer, T value, boolean withType) throws BreezeException;

    default void putType(BreezeBuffer buffer) throws BreezeException {
        buffer.put(getType());
    }

    default void write(BreezeBuffer buffer, T value) throws BreezeException {
        write(buffer, value, true);
    }

    default T read(BreezeBuffer buffer) throws BreezeException {
        return read(buffer, true);
    }

    default void writeMessageField(BreezeBuffer buffer, int index, T field) throws BreezeException {
        writeMessageField(buffer, index, field, true, true);
    }

    default void writeMessageField(BreezeBuffer buffer, int index, T field, boolean withType, boolean checkDefault) throws BreezeException {
        if (field != null) {
            buffer.putZigzag32(index);
            write(buffer, field, withType);
        }
    }
}
