package com.weibo.breeze;

/**
 * Created by zhanglei28 on 2019/3/22.
 */
@SuppressWarnings("all")
public class BreezeType {
    public static final byte MAX_DIRECT_MESSAGE_TYPE_REF = 70;

    // type define
    public static final byte NULL = 0;
    public static final byte TRUE = 1;
    public static final byte FALSE = 2;
    public static final byte STRING = 3;
    public static final byte BYTE = 4;
    public static final byte BYTE_ARRAY = 5;
    public static final byte INT16 = 6;
    public static final byte INT32 = 7;
    public static final byte INT64 = 8;
    public static final byte FLOAT32 = 9;
    public static final byte FLOAT64 = 10;

    public static final byte MAP = 20;
    public static final byte ARRAY = 21;
    public static final byte MESSAGE = 22;
    public static final byte SCHEMA = 23;
    public static final byte PACKED_MAP = 24;
    public static final byte PACKED_ARRAY = 25;

    public static final byte TYPE_REF_MESSAGE = 30; //type number 31-100 is reserved for direct message type reference.

    public byte type;
    public String messageName;

    public BreezeType(byte type, String messageName) {
        this.type = type;
        this.messageName = messageName;
    }
}
