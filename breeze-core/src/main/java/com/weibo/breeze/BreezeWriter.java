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

package com.weibo.breeze;

import com.weibo.breeze.message.Message;
import com.weibo.breeze.message.Schema;
import com.weibo.breeze.serializer.Serializer;
import com.weibo.breeze.type.BreezeType;
import com.weibo.breeze.type.TypePackedArray;
import com.weibo.breeze.type.TypePackedMap;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Map;

import static com.weibo.breeze.type.Types.*;


/**
 * @author zhanglei28
 * @date 2019/3/21.
 */
public class BreezeWriter {
    public static int MAX_WRITE_COUNT = 0; // default not check circular reference.
    public static boolean IS_PACK = true;

    public static void writeString(BreezeBuffer buffer, String str) throws BreezeException {
        TYPE_STRING.write(buffer, str);
    }

    public static void writeBytes(BreezeBuffer buffer, byte[] value) throws BreezeException {
        TYPE_BYTE_ARRAY.write(buffer, value);
    }

    public static void writeBool(BreezeBuffer buffer, boolean value) throws BreezeException {
        TYPE_BOOL.write(buffer, value);
    }

    public static void writeByte(BreezeBuffer buffer, byte value) throws BreezeException {
        TYPE_BYTE.write(buffer, value);
    }

    public static void writeInt16(BreezeBuffer buffer, short value) throws BreezeException {
        TYPE_INT16.write(buffer, value);
    }

    public static void writeInt32(BreezeBuffer buffer, int value) throws BreezeException {
        TYPE_INT32.write(buffer, value);
    }

    public static void writeInt64(BreezeBuffer buffer, long value) throws BreezeException {
        TYPE_INT64.write(buffer, value);
    }

    public static void writeFloat32(BreezeBuffer buffer, float value) throws BreezeException {
        TYPE_FLOAT32.write(buffer, value);
    }

    public static void writeFloat64(BreezeBuffer buffer, double value) throws BreezeException {
        TYPE_FLOAT64.write(buffer, value);
    }

    public static void writeMessage(BreezeBuffer buffer, WriteField writeField) throws BreezeException {
        int pos = buffer.position();
        buffer.position(pos + 4);
        writeField.writeIndexFields();
        int newPos = buffer.position();
        buffer.position(pos);
        buffer.putInt(newPos - pos - 4);
        buffer.position(newPos);
    }


    public static void writeMessageField(BreezeBuffer buffer, Integer index, Object field) throws BreezeException {
        if (field != null) {
            buffer.putVarint(index);
            writeObject(buffer, field);
        }
    }

    public static void writeSchema(BreezeBuffer buffer, Schema schema) throws BreezeException {
        //TODO
    }

    // always with type
    @SuppressWarnings("unchecked")
    public static void writeObject(BreezeBuffer buffer, Object object) throws BreezeException {
        if (object == null) {
            buffer.put(NULL);
            return;
        }
        if (object instanceof Message) {
            checkWriteCount(buffer, object);
            Message message = (Message) object;
            putMessageType(buffer, message.messageName());
            message.writeToBuf(buffer);
            return;
        }

        Class<?> clz = object.getClass();
        if (clz == String.class || clz == Character.class) {
            writeString(buffer, String.valueOf(object));
            return;
        }

        if (clz == Integer.class) {
            writeInt32(buffer, (Integer) object);
            return;
        }

        if (object instanceof Map) {
            checkWriteCount(buffer, object);
            writeMap(buffer, (Map) object);
            return;
        }

        if (object instanceof Collection) {
            checkWriteCount(buffer, object);
            writeCollection(buffer, (Collection) object);
            return;
        }

        if (clz == Boolean.class) {
            writeBool(buffer, (Boolean) object);
            return;
        }

        if (clz == Long.class) {
            writeInt64(buffer, (Long) object);
            return;
        }

        if (clz == Float.class) {
            writeFloat32(buffer, (Float) object);
            return;
        }

        if (clz == Double.class) {
            writeFloat64(buffer, (Double) object);
            return;
        }

        if (clz == Byte.class) {
            writeByte(buffer, (Byte) object);
            return;
        }

        if (clz == Short.class) {
            writeInt16(buffer, (Short) object);
            return;
        }

        if (clz.isArray()) {
            if (clz.getComponentType() == byte.class) {
                writeBytes(buffer, (byte[]) object);
            } else {
                checkWriteCount(buffer, object);
                if (clz.getComponentType().isPrimitive()) {
                    Object[] objects = new Object[Array.getLength(object)];
                    for (int i = 0; i < objects.length; i++) {
                        objects[i] = Array.get(object, i);
                    }
                    writeArray(buffer, objects);
                } else {
                    writeArray(buffer, (Object[]) object);
                }
            }
            return;
        }

        Serializer serializer = Breeze.getSerializer(object.getClass());
        if (serializer != null) {
            checkWriteCount(buffer, object);
            putMessageType(buffer, serializer.getName());
            serializer.writeToBuf(object, buffer);
            return;
        }

        throw new BreezeException("Breeze unsupported type: " + clz);
    }

    @SuppressWarnings("unchecked")
    public static void writeMap(BreezeBuffer buffer, Map<?, ?> value) throws BreezeException {
        if (IS_PACK) {
            BreezeType mapType = new TypePackedMap();
            mapType.write(buffer, value);
        } else {
            TYPE_MAP.write(buffer, value);
        }
    }

    public static void writeArray(BreezeBuffer buffer, Object[] value) throws BreezeException {
        // always not pack
        TYPE_ARRAY.writeArray(buffer, value, true);
    }

    @SuppressWarnings("unchecked")
    public static void writeCollection(BreezeBuffer buffer, Collection<?> value) throws BreezeException {
        if (IS_PACK) {
            BreezeType arrayType = new TypePackedArray();
            arrayType.write(buffer, value);
        } else {
            TYPE_ARRAY.writeCollection(buffer, value, true);
        }
    }

    public static void checkWriteCount(BreezeBuffer buffer, Object object) throws BreezeException {
        if (MAX_WRITE_COUNT > 0 && !(object instanceof Enum)) {
            int count = buffer.getContext().writeCount(System.identityHashCode(object));
            if (count > MAX_WRITE_COUNT) {
                throw new BreezeException("maybe circular reference。class:" + object.getClass());
            }
        }
    }

    public static void putMessageType(BreezeBuffer buffer, String name) throws BreezeException {
        Integer index = buffer.getContext().getMessageTypeIndex(name);
        if (index == null) {
            buffer.put(MESSAGE);
            TYPE_STRING.write(buffer, name, false);
            buffer.getContext().putMessageType(name);
        } else {
            if (index > DIRECT_REF_MESSAGE_MAX_VALUE) {// over direct type ref
                buffer.put(REF_MESSAGE);
                buffer.putVarint(index);
            } else {
                buffer.put((byte) (REF_MESSAGE + index));
            }
        }
    }

    @FunctionalInterface
    public interface WriteField {
        void writeIndexFields() throws BreezeException;
    }
}
