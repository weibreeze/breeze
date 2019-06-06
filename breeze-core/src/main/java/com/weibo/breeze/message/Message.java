package com.weibo.breeze.message;

import com.weibo.breeze.BreezeBuffer;
import com.weibo.breeze.BreezeException;

/**
 * Created by zhanglei28 on 2019/3/21.
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

    String getName();

    String getAlias();

    Message getDefaultInstance();

    Schema getSchema();
}
