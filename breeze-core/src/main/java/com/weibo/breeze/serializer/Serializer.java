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
     * @param buffer
     * @return
     * @throws BreezeException
     */
    T readFromBuf(BreezeBuffer buffer) throws BreezeException;

    /**
     * get class names or alias can be handled by serializer.
     *
     * @return
     */
    String[] getNames();
}
