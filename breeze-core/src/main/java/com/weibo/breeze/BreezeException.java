package com.weibo.breeze;

import java.io.IOException;

/**
 * Created by zhanglei28 on 2019/3/21.
 */
public class BreezeException extends IOException {
    public BreezeException(String errorMessage) {
        super(errorMessage);
    }

    public BreezeException(String errorMessage, Exception e) {
        super(errorMessage, e);
    }
}
