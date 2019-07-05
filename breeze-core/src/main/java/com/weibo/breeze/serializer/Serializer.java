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

/**
 * @author zhanglei28
 * @date 2019/3/22.
 */
public interface Serializer<T> {
    void writeToBuf(T obj, BreezeBuffer buffer) throws BreezeException;

    /**
     * read entire message from buffer.
     * the serializer need process message type, name and fields.
     *
     * @param buffer BreezeBuffer
     * @return generic object
     * @throws BreezeException serialize exception
     */
    T readFromBuf(BreezeBuffer buffer) throws BreezeException;

    // notice: the first name is write to buffer. other names for compatible.
    String[] getNames();

    // get name for writing buffer
    default String getName() {
        return getNames()[0];
    }
}
