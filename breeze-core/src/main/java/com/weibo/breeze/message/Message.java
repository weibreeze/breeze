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

package com.weibo.breeze.message;

import com.weibo.breeze.BreezeBuffer;
import com.weibo.breeze.BreezeException;

/**
 * @author zhanglei28
 * @date 2019/3/21.
 */
// a message object must has a constructor without any arguments
public interface Message {
    void writeToBuf(BreezeBuffer buffer) throws BreezeException;

    /**
     * read message fields
     *
     * @param buffer BreezeBuffer
     * @return Breeze message
     * @throws BreezeException serialize exception
     */
    Message readFromBuf(BreezeBuffer buffer) throws BreezeException;

    String messageName();

    String messageAlias();

    Message defaultInstance();

    Schema schema();
}
