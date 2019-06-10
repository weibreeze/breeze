package com.weibo.breeze.serializer;

import com.weibo.breeze.BreezeBuffer;
import com.weibo.breeze.BreezeException;

/**
 * Created by zhanglei28 on 2019/3/22.
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

    String[] getNames();
}
