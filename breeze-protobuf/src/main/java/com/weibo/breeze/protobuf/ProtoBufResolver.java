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

import com.google.protobuf.Message;
import com.weibo.breeze.Breeze;
import com.weibo.breeze.BreezeException;
import com.weibo.breeze.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhanglei28
 * @date 2019/4/2.
 */
public class ProtoBufResolver implements Breeze.SerializerResolver {
    private static final Logger logger = LoggerFactory.getLogger(ProtoBufResolver.class);

    @Override
    public Serializer getSerializer(Class<?> clz) {
        if (Message.class.isAssignableFrom(clz)) {
            try {
                return new ProtobufSerializer(clz);
            } catch (BreezeException e) {
                logger.warn("register ext serializer fail. clz:{}, e:{}", clz.getName(), e.getMessage());
            }
        }
        return null;
    }
}
