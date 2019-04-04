package com.weibo.breeze;

import com.weibo.breeze.message.Message;
import com.weibo.breeze.message.Schema;
import com.weibo.breeze.serializer.Serializer;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Map;

import static com.weibo.breeze.BreezeType.*;


/**
 * Created by zhanglei28 on 2019/3/21.
 */
@SuppressWarnings("all")
public class BreezeWriter {
    static int MAX_WRITE_COUNTE = 5; // default not check circular reference.

    public static void writeString(BreezeBuffer buffer, String str) throws BreezeException {
        buffer.put(STRING);
        byte[] b = new byte[0];
        try {
            b = str.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new BreezeException("UnsupportedEncoding UTF-8");
        }
        buffer.putZigzag32(b.length);
        buffer.put(b);
    }

    public static void writeBytes(BreezeBuffer buffer, byte[] value) {
        buffer.put(BYTE_ARRAY);
        buffer.putInt(value.length);
        buffer.put(value);
    }

    public static void writeBool(BreezeBuffer buffer, boolean value) {
        if (value) {
            buffer.put(TRUE);
        } else {
            buffer.put(FALSE);
        }
    }

    public static void writeByte(BreezeBuffer buffer, byte value) {
        buffer.put(BYTE);
        buffer.put(value);
    }

    public static void writeInt16(BreezeBuffer buffer, short value) {
        buffer.put(INT16);
        buffer.putShort(value);
    }

    public static void writeInt32(BreezeBuffer buffer, int value) {
        buffer.put(INT32);
        buffer.putZigzag32(value);
    }

    public static void writeInt64(BreezeBuffer buffer, long value) {
        buffer.put(INT64);
        buffer.putZigzag64(value);
    }

    public static void writeFloat32(BreezeBuffer buffer, float value) {
        buffer.put(FLOAT32);
        buffer.putFloat(value);
    }

    public static void writeFloat64(BreezeBuffer buffer, double value) {
        buffer.put(FLOAT64);
        buffer.putDouble(value);
    }

    public static void writeMessage(BreezeBuffer buffer, String name, Map<Integer, Object> fields) throws BreezeException {
        int pos = startWriteMessage(buffer, name);
        for (Map.Entry<Integer, Object> entry : fields.entrySet()) {
            writeMessageField(buffer, entry.getKey(), entry.getValue());
        }
        finishWriteMessage(buffer, pos);
    }


    public static void writeMessage(BreezeBuffer buffer, String name, WriteField writeField) throws BreezeException {
        int pos = startWriteMessage(buffer, name);
        writeField.writeIndexFields();
        finishWriteMessage(buffer, pos);
    }


    public static int startWriteMessage(BreezeBuffer buffer, String name) throws BreezeException {
        buffer.put(MESSAGE);
        writeString(buffer, name);
        int pos = buffer.position();
        buffer.position(pos + 4);
        return pos;
    }

    public static void writeMessageField(BreezeBuffer buffer, Integer index, Object field) throws BreezeException {
        if (index != null && field != null) {
            buffer.putZigzag32(index);
            writeObject(buffer, field);
        }
    }

    public static void finishWriteMessage(BreezeBuffer buffer, int prePosition) {
        int npos = buffer.position();
        buffer.position(prePosition);
        buffer.putInt(npos - prePosition - 4);
        buffer.position(npos);
    }

    public static void writeSchema(BreezeBuffer buffer, Schema schema) throws BreezeException {
        if (schema == null) {
            buffer.put(NULL);
        } else {
            //TODO
        }
    }

    public static void writeObject(BreezeBuffer buffer, Object object) throws BreezeException {
        if (object == null) {
            buffer.put(NULL);
            return;
        }
        Class<?> clz = object.getClass();
        if (clz == String.class || clz == char.class || clz == Character.class) {
            writeString(buffer, String.valueOf(object));
            return;
        }

        if (clz == Byte.class || clz == byte.class) {
            writeByte(buffer, (Byte) object);
            return;
        }

        if (clz == Boolean.class || clz == boolean.class) {
            writeBool(buffer, (Boolean) object);
            return;
        }

        if (clz == Short.class || clz == short.class) {
            writeInt16(buffer, (Short) object);
            return;
        }

        if (clz == Integer.class || clz == int.class) {
            writeInt32(buffer, (Integer) object);
            return;
        }

        if (clz == Long.class || clz == long.class) {
            writeInt64(buffer, (Long) object);
            return;
        }

        if (clz == Float.class || clz == float.class) {
            writeFloat32(buffer, (Float) object);
            return;
        }

        if (clz == Double.class || clz == double.class) {
            writeFloat64(buffer, (Double) object);
            return;
        }

        if (object instanceof Map) {
            checkWriteCount(buffer, object);
            writeMap(buffer, (Map) object);
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

        if (object instanceof Collection) {
            checkWriteCount(buffer, object);
            writeCollection(buffer, (Collection) object);
            return;
        }
        if (object instanceof Message) {
            checkWriteCount(buffer, object);
            ((Message) object).writeToBuf(buffer);
            return;
        }
        Serializer serializer = Breeze.getSerializer(object.getClass());
        if (serializer != null) {
            checkWriteCount(buffer, object);
            serializer.writeToBuf(object, buffer);
            return;
        }
        throw new BreezeException("Breeze unsupported type: " + clz);
    }

    public static void writeMap(BreezeBuffer buffer, Map<?, ?> value) throws BreezeException {
        buffer.put(MAP);
        int pos = buffer.position();
        buffer.position(pos + 4);
        for (Map.Entry<?, ?> entry : value.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            writeObject(buffer, entry.getKey());
            writeObject(buffer, entry.getValue());
        }
        int npos = buffer.position();
        buffer.position(pos);
        buffer.putInt(npos - pos - 4);
        buffer.position(npos);
    }

    public static void writeArray(BreezeBuffer buffer, Object[] value) throws BreezeException {
        buffer.put(ARRAY);
        int pos = buffer.position();
        buffer.position(pos + 4);
        for (int i = 0; i < value.length; i++) {
            writeObject(buffer, value[i]);
        }
        int npos = buffer.position();
        buffer.position(pos);
        buffer.putInt(npos - pos - 4);
        buffer.position(npos);
    }

    public static void writeCollection(BreezeBuffer buffer, Collection<?> value) throws BreezeException {
        buffer.put(ARRAY);
        int pos = buffer.position();
        buffer.position(pos + 4);
        for (Object v : value) {
            writeObject(buffer, v);
        }
        int npos = buffer.position();
        buffer.position(pos);
        buffer.putInt(npos - pos - 4);
        buffer.position(npos);
    }

    @FunctionalInterface
    public interface WriteField {
        void writeIndexFields() throws BreezeException;
    }

    private static void checkWriteCount(BreezeBuffer buffer, Object object) throws BreezeException {
        if (MAX_WRITE_COUNTE > 0) {
            int count = buffer.writeCount(System.identityHashCode(object));
            if (count > MAX_WRITE_COUNTE) {
                throw new BreezeException("maybe circular reference。class:" + object.getClass());
            }
        }
    }
}
