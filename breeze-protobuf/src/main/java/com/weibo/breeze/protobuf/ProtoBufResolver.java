package com.weibo.breeze.protobuf;

import com.google.protobuf.Message;
import com.weibo.breeze.Breeze;
import com.weibo.breeze.BreezeException;
import com.weibo.breeze.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhanglei28 on 2019/4/2.
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
