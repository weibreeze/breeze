package com.weibo.breeze.serializer;

import com.weibo.breeze.BreezeException;

import java.util.Map;

/**
 * Created by zhanglei28 on 2019/3/26.
 */
public interface SerializerFactory {
    Serializer getSerializer(Class clz);

    Serializer getSerializer(String name);

    Serializer removeSerializer(String name);

    void registerSerializer(Serializer serializer) throws BreezeException;

    void registerSerializer(String name, Serializer serializer) throws BreezeException;

    Map<String, Serializer> getSerializers();
}
