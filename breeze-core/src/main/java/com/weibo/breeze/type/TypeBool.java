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

import static com.weibo.breeze.type.Types.FALSE;
import static com.weibo.breeze.type.Types.TRUE;

/**
 * @author zhanglei28
 * @date 2019/7/3.
 */
public class TypeBool implements BreezeType<Boolean> {
    TypeBool() {
    }

    public static boolean readBool(BreezeBuffer buffer) throws BreezeException {
        byte b = buffer.get();
        if (b == TRUE) {
            return true;
        } else if (b == FALSE) {
            return false;
        }
        throw new BreezeException("boolean type not correct. expect type:" + TRUE + " or " + FALSE + ", real type:" + b);
    }

    @Override
    public byte getType() {
        return TRUE;
    }

    @Override
    public Boolean read(BreezeBuffer buffer, boolean withType) throws BreezeException {
        return readBool(buffer);
    }

    @Override
    public void write(BreezeBuffer buffer, Boolean value, boolean withType) throws BreezeException {
        if (value) {
            buffer.put(TRUE);
        } else {
            buffer.put(FALSE);
        }
    }

    public void writeMessageField(BreezeBuffer buffer, int index, Boolean field, boolean withType, boolean checkDefault) throws BreezeException {
        if (field != null) {
            if (field) {
                buffer.putZigzag32(index);
                write(buffer, field, withType);
            }
        }
    }
}
