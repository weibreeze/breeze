package com.weibo.breeze;

import com.weibo.breeze.message.Message;
import com.weibo.breeze.message.Schema;
import com.weibo.breeze.serializer.Serializer;

import java.lang.reflect.Array;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Map;

import static com.weibo.breeze.BreezeType.*;


/**
 * Created by zhanglei28 on 2019/3/21.
 */
@SuppressWarnings("all")
public class BreezeWriter {
    static int MAX_WRITE_COUNT = 10; // default not check circular reference.
    static Charset charset = Charset.forName("UTF-8");
    static byte[] tempBuf = new byte[1024];

    public static void writeString(BreezeBuffer buffer, String str, boolean withType) throws BreezeException {
        if (withType) {
            buffer.put(STRING);
        }
        buffer.putUTF8(str);
//        int len = Utf8.encode(str, tempBuf, 0, 1024);
//        buffer.putZigzag32(len);
//        buffer.put(tempBuf, 0 , len);


//        byte[] b  = str.getBytes(charset);
//        buffer.putZigzag32(b.length);
//        buffer.put(b);
    }

    public static void writeBytes(BreezeBuffer buffer, byte[] value, boolean withType) {
        if (withType) {
            buffer.put(BYTE_ARRAY);
        }
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

    public static void writeByte(BreezeBuffer buffer, byte value, boolean withType) {
        if (withType) {
            buffer.put(BYTE);
        }
        buffer.put(value);
    }

    public static void writeInt16(BreezeBuffer buffer, short value, boolean withType) {
        if (withType) {
            buffer.put(INT16);
        }
        buffer.putShort(value);
    }

    public static void writeInt32(BreezeBuffer buffer, int value, boolean withType) {
        if (withType) {
            buffer.put(INT32);
        }
        buffer.putZigzag32(value);
    }

    public static void writeInt64(BreezeBuffer buffer, long value, boolean withType) {
        if (withType) {
            buffer.put(INT64);
        }
        buffer.putZigzag64(value);
    }

    public static void writeFloat32(BreezeBuffer buffer, float value, boolean withType) {
        if (withType) {
            buffer.put(FLOAT32);
        }
        buffer.putFloat(value);
    }

    public static void writeFloat64(BreezeBuffer buffer, double value, boolean withType) {
        if (withType) {
            buffer.put(FLOAT64);
        }
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
        if (buffer.getContext().withType) {
            putMessageType(buffer, name);
        }

        int pos = buffer.position();
        buffer.position(pos + 4);
        return pos;
    }

    public static void writeMessageField(BreezeBuffer buffer, Integer index, Object field) throws BreezeException {
        writeMessageField(buffer, index, field, true, true, true);
    }

    public static void writeMessageField(BreezeBuffer buffer, Integer index, Object field, boolean checkDefault, boolean withType, boolean isPack) throws BreezeException {
        if (field != null) {
            if (checkDefault) { // not write if field is default value
                if (field instanceof Number && ((Number) field).intValue() == 0) {
                    return;
                }
                if (field instanceof Boolean && !((Boolean) field).booleanValue()) {
                    return;
                }
            }
            buffer.putZigzag32(index);
            writeObject(buffer, field, withType, isPack);
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
        writeObject(buffer, object, true, true);
    }


    public static void writeObject(BreezeBuffer buffer, Object object, boolean withType, boolean isPack) throws BreezeException {
        if (object == null) {
            buffer.put(NULL);
            return;
        }
        Class<?> clz = object.getClass();
        if (clz == String.class || clz == char.class || clz == Character.class) {
            writeString(buffer, String.valueOf(object), withType);
            return;
        }

        if (clz == Integer.class || clz == int.class) {
            writeInt32(buffer, (Integer) object, withType);
            return;
        }

        if (object instanceof Map) {
            checkWriteCount(buffer, object);
            writeMap(buffer, (Map) object, withType, isPack);
            return;
        }

        if (clz == Byte.class || clz == byte.class) {
            writeByte(buffer, (Byte) object, withType);
            return;
        }

        if (clz == Boolean.class || clz == boolean.class) {
            writeBool(buffer, (Boolean) object);
            return;
        }

        if (clz == Short.class || clz == short.class) {
            writeInt16(buffer, (Short) object, withType);
            return;
        }

        if (clz == Long.class || clz == long.class) {
            writeInt64(buffer, (Long) object, withType);
            return;
        }

        if (clz == Float.class || clz == float.class) {
            writeFloat32(buffer, (Float) object, withType);
            return;
        }

        if (clz == Double.class || clz == double.class) {
            writeFloat64(buffer, (Double) object, withType);
            return;
        }

        if (clz.isArray()) {
            if (clz.getComponentType() == byte.class) {
                writeBytes(buffer, (byte[]) object, withType);
            } else {
                checkWriteCount(buffer, object);
                if (clz.getComponentType().isPrimitive()) {
                    Object[] objects = new Object[Array.getLength(object)];
                    for (int i = 0; i < objects.length; i++) {
                        objects[i] = Array.get(object, i);
                    }
                    writeArray(buffer, objects, withType, isPack);
                } else {
                    writeArray(buffer, (Object[]) object, withType, isPack);
                }
            }
            return;
        }

        if (object instanceof Collection) {
            checkWriteCount(buffer, object);
            writeCollection(buffer, (Collection) object, withType, isPack);
            return;
        }
        if (object instanceof Message) {
            checkWriteCount(buffer, object);
            boolean oldValue = buffer.getContext().withType;
            buffer.getContext().withType = withType;
            ((Message) object).writeToBuf(buffer);
            buffer.getContext().withType = oldValue;
            return;
        }
        Serializer serializer = Breeze.getSerializer(object.getClass());
        if (serializer != null) {
            checkWriteCount(buffer, object);
            boolean oldValue = buffer.getContext().withType;
            buffer.getContext().withType = withType;
            serializer.writeToBuf(object, buffer);
            buffer.getContext().withType = oldValue;
            return;
        }

        throw new BreezeException("Breeze unsupported type: " + clz);
    }

    public static void writeMap(BreezeBuffer buffer, Map<?, ?> value, boolean withType, boolean isPack) throws BreezeException {
        if (withType) {
            if (isPack) {
                buffer.put(PACKED_MAP);
            } else {
                buffer.put(MAP);
            }
        }
        if (value.isEmpty()) {
            buffer.putInt(0);
            return;
        }
        int pos = buffer.position();
        buffer.position(pos + 4);
        boolean typeWrited = false;
        for (Map.Entry<?, ?> entry : value.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            if (isPack && !typeWrited) {
                putType(buffer, entry.getKey());
                putType(buffer, entry.getValue());
                typeWrited = true;
            }
            writeObject(buffer, entry.getKey(), !isPack, isPack);
            writeObject(buffer, entry.getValue(), !isPack, isPack);
        }
        int npos = buffer.position();
        buffer.position(pos);
        buffer.putInt(npos - pos - 4);
        buffer.position(npos);
    }

    public static void writeArray(BreezeBuffer buffer, Object[] value, boolean withType, boolean isPack) throws BreezeException {
        if (withType) {
            if (isPack) {
                buffer.put(PACKED_ARRAY);
            } else {
                buffer.put(ARRAY);
            }
        }
        if (value.length == 0) {
            buffer.putInt(0);
            return;
        }
        int pos = buffer.position();
        buffer.position(pos + 4);
        boolean typeWrited = false;
        for (int i = 0; i < value.length; i++) {
            if (isPack) {
                if (value[i] == null) {
                    continue; // packed array not process null value
                }
                if (!typeWrited) {
                    putType(buffer, value[i]);
                    typeWrited = true;
                }
            }
            writeObject(buffer, value[i], !isPack, isPack);
        }
        int npos = buffer.position();
        buffer.position(pos);
        buffer.putInt(npos - pos - 4);
        buffer.position(npos);
    }

    public static void writeCollection(BreezeBuffer buffer, Collection<?> value, boolean withType, boolean isPack) throws BreezeException {
        if (withType) {
            if (isPack) {
                buffer.put(PACKED_ARRAY);
            } else {
                buffer.put(ARRAY);
            }
        }
        if (value.isEmpty()) {
            buffer.putInt(0);
            return;
        }
        int pos = buffer.position();
        buffer.position(pos + 4);
        boolean typeWrited = false;
        for (Object v : value) {
            if (isPack) {
                if (v == null) {
                    continue;// packed array not process null value
                }
                if (!typeWrited) {
                    putType(buffer, v);
                    typeWrited = true;
                }
            }
            writeObject(buffer, v, !isPack, isPack);
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
        if (MAX_WRITE_COUNT > 0 && !(object instanceof Enum)) {
            int count = buffer.getContext().writeCount(System.identityHashCode(object));
            if (count > MAX_WRITE_COUNT) {
                throw new BreezeException("maybe circular reference。class:" + object.getClass());
            }
        }
    }

    private static void putType(BreezeBuffer buffer, Object object) throws BreezeException {
        Class<?> clz = object.getClass();
        if (clz == String.class || clz == char.class || clz == Character.class) {
            buffer.put(STRING);
            return;
        }

        if (clz == Byte.class || clz == byte.class) {
            buffer.put(BYTE);
            return;
        }

        if (clz == Boolean.class || clz == boolean.class) {
            buffer.put(TRUE);
            return;
        }

        if (clz == Short.class || clz == short.class) {
            buffer.put(INT16);
            return;
        }

        if (clz == Integer.class || clz == int.class) {
            buffer.put(INT32);
            return;
        }

        if (clz == Long.class || clz == long.class) {
            buffer.put(INT64);
            return;
        }

        if (clz == Float.class || clz == float.class) {
            buffer.put(FLOAT32);
            return;
        }

        if (clz == Double.class || clz == double.class) {
            buffer.put(FLOAT64);
            return;
        }

        if (object instanceof Map) {
            buffer.put(PACKED_MAP);
            return;
        }

        if (clz.isArray()) {
            if (clz.getComponentType() == byte.class) {
                buffer.put(BYTE_ARRAY);
            } else {
                buffer.put(PACKED_ARRAY);
            }
            return;
        }

        if (object instanceof Collection) {
            buffer.put(PACKED_ARRAY);
            return;
        }
        if (object instanceof Message) {
            putMessageType(buffer, ((Message) object).getName());
            return;
        }
        Serializer serializer = Breeze.getSerializer(object.getClass());
        if (serializer != null) {
            if (serializer.getNames() == null || serializer.getNames().length == 0) {
                throw new BreezeException("breeze serializer must has name. serializer:" + serializer.getClass().getName());
            }
            putMessageType(buffer, serializer.getNames()[0]);
            return;
        }
        throw new BreezeException("Breeze unsupported type: " + clz);
    }

    private static void putMessageType(BreezeBuffer buffer, String name) throws BreezeException {
        Integer index = buffer.getContext().getMessageTypeIndex(name);
        if (index == null) {
            buffer.put(MESSAGE);
            writeString(buffer, name, false);
            buffer.getContext().putMessageType(name);
        } else {
            if (index > MAX_DIRECT_MESSAGE_TYPE_REF) {// over direct type ref
                buffer.put(TYPE_REF_MESSAGE);
                buffer.putZigzag32(index);
            } else {
                buffer.put((byte) (TYPE_REF_MESSAGE + index));
            }
        }
    }
}
