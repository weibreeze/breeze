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

/**
 * @author zhanglei28
 * @date 2019/7/2.
 */
public class Types {
    // type define
    public static final byte STRING = 0x3f; // e.g. direct_string_max + 1
    public static final byte DIRECT_STRING_MIN_TYPE = 0x00; // empty string
    public static final byte DIRECT_STRING_MAX_TYPE = 0x3e;
    public static final byte INT32 = 0x7f;
    public static final byte INT32_ZERO = 0x50;
    public static final byte DIRECT_INT32_MIN_TYPE = 0x40;
    public static final byte DIRECT_INT32_MAX_TYPE = 0x7e;
    public static final byte INT64 = -0x68;
    public static final byte INT64_ZERO = -0x78;
    public static final byte DIRECT_INT64_MIN_TYPE = -0x80;
    public static final byte DIRECT_INT64_MAX_TYPE = -0x69;
    public static final byte NULL = -0x67;
    public static final byte TRUE = -0x66;
    public static final byte FALSE = -0x65;
    public static final byte BYTE = -0x64;
    public static final byte BYTE_ARRAY = -0x63;
    public static final byte INT16 = -0x62;
    public static final byte FLOAT32 = -0x61;
    public static final byte FLOAT64 = -0x60;
    public static final byte MAP = -0x27;
    public static final byte ARRAY = -0x26;
    public static final byte PACKED_MAP = -0x25;
    public static final byte PACKED_ARRAY = -0x24;
    public static final byte SCHEMA = -0x23;
    public static final byte MESSAGE = -0x22;
    public static final byte REF_MESSAGE = -0x21; //reference of message type.
    public static final byte DIRECT_REF_MESSAGE_MAX_TYPE = -0x01; //max reference of message type.

    public static final int DIRECT_STRING_MAX_LENGTH = DIRECT_STRING_MAX_TYPE;
    public static final int DIRECT_INT32_MIN_VALUE = DIRECT_INT32_MIN_TYPE - INT32_ZERO;
    public static final int DIRECT_INT32_MAX_VALUE = DIRECT_INT32_MAX_TYPE - INT32_ZERO;
    public static final int DIRECT_INT64_MIN_VALUE = DIRECT_INT64_MIN_TYPE - INT64_ZERO;
    public static final int DIRECT_INT64_MAX_VALUE = DIRECT_INT64_MAX_TYPE - INT64_ZERO;
    public static final int DIRECT_REF_MESSAGE_MAX_VALUE = DIRECT_REF_MESSAGE_MAX_TYPE - REF_MESSAGE;

    //singleton of basic type
    public static final TypeInt16 TYPE_INT16 = new TypeInt16();
    public static final TypeInt32 TYPE_INT32 = new TypeInt32();
    public static final TypeInt64 TYPE_INT64 = new TypeInt64();
    public static final TypeFloat32 TYPE_FLOAT32 = new TypeFloat32();
    public static final TypeFloat64 TYPE_FLOAT64 = new TypeFloat64();
    public static final TypeBool TYPE_BOOL = new TypeBool();
    public static final TypeByte TYPE_BYTE = new TypeByte();
    public static final TypeByteArray TYPE_BYTE_ARRAY = new TypeByteArray();
    public static final TypeString TYPE_STRING = new TypeString();
    public static final TypeMap TYPE_MAP = new TypeMap();
    public static final TypeArray TYPE_ARRAY = new TypeArray();
}
