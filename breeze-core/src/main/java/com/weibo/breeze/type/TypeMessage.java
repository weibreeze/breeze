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

import com.weibo.breeze.Breeze;
import com.weibo.breeze.BreezeBuffer;
import com.weibo.breeze.BreezeException;
import com.weibo.breeze.BreezeReader;
import com.weibo.breeze.message.Message;
import com.weibo.breeze.serializer.Serializer;

import static com.weibo.breeze.BreezeWriter.checkWriteCount;
import static com.weibo.breeze.BreezeWriter.putMessageType;
import static com.weibo.breeze.type.Types.MESSAGE;
import static com.weibo.breeze.type.Types.NULL;

/**
 * @author zhanglei28
 * @date 2019/7/3.
 */
public class TypeMessage<T> implements BreezeType<T> {
    private Message defaultMessage;
    private Serializer<T> serializer;
    private String name;

    @SuppressWarnings("unchecked")
    public TypeMessage(Class<T> clz) throws BreezeException {
        if (clz == null) {
            throw new BreezeException("class must not null in TypeMessage");
        }
        if (Message.class.isAssignableFrom(clz)) {
            try {
                defaultMessage = (Message) clz.newInstance();
                name = defaultMessage.getName();
            } catch (Exception e) {
                throw new BreezeException("create new default Message fail. Message must have a constructor without arguments. e:" + e.getMessage());
            }
        } else {// check serializer
            serializer = Breeze.getSerializer(clz);
            if (serializer == null) {
                throw new BreezeException("can not find breeze serializer for class: " + clz.getName());
            }
            name = serializer.getNames()[0];
        }
    }

    public TypeMessage(Message defaultMessage) throws BreezeException {
        if (defaultMessage == null) {
            throw new BreezeException("default Message must not null in TypeMessage");
        }
        this.defaultMessage = defaultMessage;
        this.name = defaultMessage.getName();
    }

    public TypeMessage(Serializer<T> serializer) throws BreezeException {
        if (serializer == null) {
            throw new BreezeException("serializer must not null in TypeMessage");
        }
        this.serializer = serializer;
        this.name = serializer.getNames()[0];
    }

    @Override
    public byte getType() {
        return MESSAGE;
    }

    @Override
    public void putType(BreezeBuffer buffer) throws BreezeException {
        putMessageType(buffer, name);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T read(BreezeBuffer buffer, boolean withType) throws BreezeException {
        if (withType) {
            // need check type?
            byte type = buffer.get();
            if (type == NULL) {
                return null;
            }
            BreezeReader.readMessageName(buffer, type);
        }
        if (defaultMessage != null) {
            return (T) defaultMessage.getDefaultInstance().readFromBuf(buffer);
        }
        return serializer.readFromBuf(buffer);
    }

    @Override
    public void write(BreezeBuffer buffer, T value, boolean withType) throws BreezeException {
        checkWriteCount(buffer, value);
        if (withType) {
            putMessageType(buffer, name);
        }
        if (value instanceof Message) {
            ((Message) value).writeToBuf(buffer);
        } else {
            serializer.writeToBuf(value, buffer);
        }
    }

}
