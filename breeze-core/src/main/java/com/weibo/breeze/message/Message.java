package com.weibo.breeze.message;

import com.weibo.breeze.BreezeException;
import com.weibo.breeze.BreezeBuffer;

/**
 * Created by zhanglei28 on 2019/3/21.
 */
// a message object must has a constructor without any arguments
public interface Message {
    void writeToBuf(BreezeBuffer buffer) throws BreezeException;

    /**
     * read message fields
     * @param buffer
     * @return
     * @throws BreezeException
     */
    Message readFromBuf(BreezeBuffer buffer) throws BreezeException;

    String getName();

    String getAlias();

    Message getDefaultInstance();

    Schema getSchema();
}
