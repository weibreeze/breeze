package com.weibo.breeze;

import com.weibo.breeze.message.GenericMessage;
import com.weibo.breeze.message.Message;
import com.weibo.breeze.message.Schema;
import com.weibo.breeze.serializer.CommonSerializer;
import com.weibo.breeze.serializer.Serializer;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

import static com.weibo.breeze.BreezeType.*;

/**
 * Created by zhanglei28 on 2019/3/21.
 */
@SuppressWarnings("all")
public class BreezeReader {

    public static Boolean readBool(BreezeBuffer buffer) throws BreezeException {
        byte b = buffer.get();
        if (b == TRUE) {
            return true;
        } else if (b == FALSE) {
            return false;
        }
        throw new BreezeException("message type not correct. expect type:" + TRUE + " or " + FALSE + ", real type:" + b);
    }

    public static Byte readByte(BreezeBuffer buffer) throws BreezeException {
        checkType(buffer, BYTE);
        return readByteWithoutType(buffer);
    }

    public static Short readInt16(BreezeBuffer buffer) throws BreezeException {
        checkType(buffer, INT16);
        return readInt16WithoutType(buffer);
    }

    public static Integer readInt32(BreezeBuffer buffer) throws BreezeException {
        checkType(buffer, INT32);
        return readInt32WithoutType(buffer);
    }

    public static Long readInt64(BreezeBuffer buffer) throws BreezeException {
        checkType(buffer, INT64);
        return readInt64WithoutType(buffer);
    }

    public static Float readFloat32(BreezeBuffer buffer) throws BreezeException {
        checkType(buffer, FLOAT32);
        return readFloat32WithoutType(buffer);
    }

    public static Double readFloat64(BreezeBuffer buffer) throws BreezeException {
        checkType(buffer, FLOAT64);
        return readFloat64WithoutType(buffer);
    }

    public static String readString(BreezeBuffer buffer) throws BreezeException {
        checkType(buffer, STRING);
        return readStringWithoutType(buffer);
    }

    public static byte[] readBytes(BreezeBuffer buffer) throws BreezeException {
        checkType(buffer, BYTE_ARRAY);
        return readBytesWithoutType(buffer, false);
    }

    public static <T, K> void readMap(BreezeBuffer buffer, Map<T, K> map, Class<T> keyClz, Class<K> valueClz) throws BreezeException {
        readMapByType(buffer, map, keyClz, valueClz);
    }

    // with generic type
    public static <T, K> void readMapByType(BreezeBuffer buffer, Map<T, K> map, Type keyType, Type valueType) throws BreezeException {
        checkType(buffer, MAP);
        readMapWithoutType(buffer, map, keyType, valueType);
    }

    public static Map readMap(BreezeBuffer buffer) throws BreezeException {
        Map result = new HashMap<>();
        readMap(buffer, result, Object.class, Object.class);
        return result;
    }

    public static <T> void readCollection(BreezeBuffer buffer, Collection<T> collection, Class<T> clz) throws BreezeException {
        readCollectionByType(buffer, collection, clz);
    }

    // with generic type
    public static <T> void readCollectionByType(BreezeBuffer buffer, Collection<T> collection, Type type) throws BreezeException {
        checkType(buffer, ARRAY);
        readCollectionWithoutType(buffer, collection, type);
    }

    public static Message readMessage(BreezeBuffer buffer, Class<? extends Message> clz) throws BreezeException {
        checkType(buffer, MESSAGE);
        return readMessageWithoutType(buffer, clz);
    }

    public static void readMessage(BreezeBuffer buffer, boolean checkType, ReadField readField) throws BreezeException {
        if (checkType) {
            byte msgType = buffer.get();
            if (msgType != BreezeType.MESSAGE) {
                throw new BreezeException("can not read type " + msgType);
            }
            BreezeReader.readString(buffer); // skip name
        }
        int size = BreezeReader.getAndCheckSize(buffer);
        if (size == 0) {
            return;
        }
        int startPos = buffer.position();
        int endPos = startPos + size;
        int index;
        while (buffer.position() < endPos) {
            index = buffer.getZigzag32();
            readField.readIndexField(index);
        }
        if (buffer.position() != endPos) {
            throw new RuntimeException("Breeze deserialize wrong map size, except: " + size + " actual: " + (buffer.position() - startPos));
        }
    }

    public static void readFields(BreezeBuffer buffer, Map<Integer, Object> fields) throws BreezeException {
        int size = getAndCheckSize(buffer);
        if (size == 0) {
            return;
        }
        int startPos = buffer.position();
        int endPos = startPos + size;
        while (buffer.position() < endPos) {
            fields.put(buffer.getZigzag32(), readObject(buffer, Object.class));
        }
        if (buffer.position() != endPos) {
            throw new RuntimeException("Breeze deserialize wrong map size, except: " + size + " actual: " + (buffer.position() - startPos));
        }
    }

    public static Schema readSchema(BreezeBuffer buffer) throws BreezeException {
        checkType(buffer, SCHEMA);
        return readSchemaWithOutType(buffer);
    }

    private static void checkType(BreezeBuffer buffer, byte type) throws BreezeException {
        byte bufType = buffer.get();
        if (bufType != type) {
            throw new BreezeException("message type not correct. expect type:" + type + ", real type:" + bufType);
        }
    }

    public static <T> T readObject(BreezeBuffer buffer, Class<T> clz) throws BreezeException {
        if (clz == null) {
            throw new BreezeException("class type must not null");
        }
        return (T) readObjectByType(buffer, clz);
    }

    public static Object readObjectByType(BreezeBuffer buffer, Type type) throws BreezeException {
        if (type == null) {
            type = Object.class;
        }
        Class clz;
        ParameterizedType pt = null;
        if (type instanceof Class) {
            clz = (Class) type;
        } else if (type instanceof ParameterizedType) {
            pt = (ParameterizedType) type;
            clz = (Class) pt.getRawType();
        } else {
            throw new BreezeException("unknown read type :" + type);
        }
        Type keyType = Object.class;
        Type valueType = Object.class;
        Object o = null;
        Serializer serializer;
        byte breezeType = buffer.get();
        switch (breezeType) {
            case NULL:
                return null;
            case STRING:
                String string = readStringWithoutType(buffer);
                if (clz == String.class || clz == Object.class) {
                    return string;
                }
                o = adaptFromString(string, clz);
                break;
            case BYTE_ARRAY:
                byte[] bytes = readBytesWithoutType(buffer, false);
                if (clz == byte[].class || clz == Object.class) {
                    return bytes;
                }
                break;
            case TRUE:
                if (clz == boolean.class || clz == Boolean.class || clz == Object.class) {
                    return Boolean.TRUE;
                }
                break;
            case FALSE:
                if (clz == boolean.class || clz == Boolean.class || clz == Object.class) {
                    return Boolean.FALSE;
                }
                break;
            case BYTE:
                Byte aByte = readByteWithoutType(buffer);
                if (clz == byte.class || clz == Byte.class || clz == Object.class) {
                    return aByte;
                }
                break;
            case INT16:
                Short aShort = readInt16WithoutType(buffer);
                if (clz == short.class || clz == Short.class || clz == Object.class) {
                    return aShort;
                }
                o = adaptFromNumber(aShort, clz);
                break;
            case INT32:
                Integer integer = readInt32WithoutType(buffer);
                if (clz == int.class || clz == Integer.class || clz == Object.class) {
                    return integer;
                }
                o = adaptFromNumber(integer, clz);
                break;
            case INT64:
                Long aLong = readInt64WithoutType(buffer);
                if (clz == long.class || clz == Long.class || clz == Object.class) {
                    return aLong;
                }
                o = adaptFromNumber(aLong, clz);
                break;
            case FLOAT32:
                Float aFloat = readFloat32WithoutType(buffer);
                if (clz == float.class || clz == Float.class || clz == Object.class) {
                    return aFloat;
                }
                o = adaptFromNumber(aFloat, clz);
                break;
            case FLOAT64:
                Double aDouble = readFloat64WithoutType(buffer);
                if (clz == double.class || clz == Double.class || clz == Object.class) {
                    return aDouble;
                }
                o = adaptFromNumber(aDouble, clz);
                break;
            case MAP:
                if (pt != null && pt.getActualTypeArguments().length == 2) {
                    keyType = pt.getActualTypeArguments()[0];
                    valueType = pt.getActualTypeArguments()[1];
                }
                if (clz.isAssignableFrom(HashMap.class)) {
                    // contain Object, Map
                    Map map = new HashMap<>();
                    readMapWithoutType(buffer, map, keyType, valueType);
                    return map;
                }
                if (!clz.isInterface() && Map.class.isAssignableFrom(clz)) {
                    Map map = null;
                    try {
                        map = (Map) clz.newInstance();
                    } catch (Exception ignore) {
                    }
                    if (map != null) {
                        readMapWithoutType(buffer, map, keyType, valueType);
                        return map;
                    }
                }
                break;
            case ARRAY:
                if (pt != null && pt.getActualTypeArguments().length == 1) {
                    valueType = pt.getActualTypeArguments()[0];
                }
                if (clz.isArray()) {
                    List list = new ArrayList();
                    readCollectionWithoutType(buffer, list, clz.getComponentType());
                    Object objects = Array.newInstance(clz.getComponentType(), list.size());
                    for (int i = 0; i < list.size(); i++) {
                        Array.set(objects, i, list.get(i));
                    }
                    return objects;
                }
                if (clz.isAssignableFrom(ArrayList.class)) {
                    List list = new ArrayList();
                    readCollectionWithoutType(buffer, list, valueType);
                    return list;
                }
                if (clz.isAssignableFrom(HashSet.class)) {
                    HashSet hs = new HashSet();
                    readCollectionWithoutType(buffer, hs, valueType);
                    return hs;
                }
                if (!clz.isInterface() && Collection.class.isAssignableFrom(clz)) {
                    Collection collection = null;
                    try {
                        collection = (Collection) clz.newInstance();
                    } catch (Exception ignore) {
                    }
                    if (collection != null) {
                        readCollectionWithoutType(buffer, collection, valueType);
                        return collection;
                    }
                }
                break;
            case MESSAGE:
                if (Message.class.isAssignableFrom(clz)) {
                    return readMessageWithoutType(buffer, (Class<Message>) clz);
                }
                if (clz == Object.class || clz.isInterface()) {
                    int pos = buffer.position();
                    String name = readString(buffer);
                    Message message = Breeze.getMessageInstance(name);
                    if (message != null) {
                        return message.readFromBuf(buffer);
                    }
                    serializer = Breeze.getSerializer(name); // direct register serializer
                    if (serializer == null) {// has breeze schema file
                        Schema schema = SchemaLoader.loadSchema(name);
                        if (schema != null) {
                            serializer = new CommonSerializer(schema);
                        }
                    }
                    if (serializer != null) {
                        buffer.position(pos);
                        return readBySerializer(buffer, serializer);
                    }
                    if (clz == Object.class) {
                        GenericMessage genericMessage = new GenericMessage();
                        genericMessage.setName(name);
                        genericMessage.readFromBuf(buffer);
                        return genericMessage;
                    }
                }
                break;
            case SCHEMA:
                if (clz == Schema.class) {
                    return readSchemaWithOutType(buffer);
                } else {
                    //TODO autoscan register？
                }
                break;
            default:
                throw new RuntimeException("Breeze not support " + breezeType + " with receiver type:" + clz);
        }
        if (o != null) {
            return o;
        }
        //
        serializer = Breeze.getSerializer(clz);
        if (serializer != null) {
            return readBySerializer(buffer, serializer);
        }
        throw new RuntimeException("Breeze not support " + breezeType + " with receiver type:" + clz);
    }

    private static <T> T adaptFromString(String string, Class<T> clz) {
        if (clz == boolean.class || clz == Boolean.class) {
            return (T) Boolean.valueOf(string);
        }
        if (clz == byte[].class || clz == Object.class) {
            try {
                return (T) string.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                return null;
            }
        }
        if (clz == short.class || clz == Short.class) {
            return (T) Short.valueOf(string);
        }
        if ((clz == int.class || clz == Integer.class)) {
            return (T) Integer.valueOf(string);
        }
        if (clz == long.class || clz == Long.class) {
            return (T) Long.valueOf(string);
        }
        if (clz == float.class || clz == Float.class) {
            return (T) Float.valueOf(string);
        }
        if (clz == double.class || clz == Double.class) {
            return (T) Double.valueOf(string);
        }
        if ((clz == char.class || clz == Character.class) && string.length() == 1) {
            return (T) new Character(string.charAt(0));
        }
        return null;
    }

    private static <T> T adaptFromNumber(Number number, Class<T> clz) {
        if (clz == short.class || clz == Short.class) {
            return (T) Short.valueOf(number.shortValue());
        }
        if ((clz == int.class || clz == Integer.class)) {
            return (T) Integer.valueOf(number.intValue());
        }
        if (clz == long.class || clz == Long.class) {
            return (T) Long.valueOf(number.longValue());
        }
        if (clz == float.class || clz == Float.class) {
            return (T) Float.valueOf(number.floatValue());
        }
        if (clz == double.class || clz == Double.class) {
            return (T) Double.valueOf(number.doubleValue());
        }
        return null;
    }

    private static Object readBySerializer(BreezeBuffer buffer, Serializer serializer) throws BreezeException {
        buffer.position(buffer.position() - 1);
        return serializer.readFromBuf(buffer);
    }

    private static Byte readByteWithoutType(BreezeBuffer buffer) {
        return buffer.get();
    }

    private static Short readInt16WithoutType(BreezeBuffer buffer) {
        return buffer.getShort();
    }

    private static Integer readInt32WithoutType(BreezeBuffer buffer) {
        return buffer.getZigzag32();
    }

    private static Long readInt64WithoutType(BreezeBuffer buffer) {
        return buffer.getZigzag64();
    }

    private static Float readFloat32WithoutType(BreezeBuffer buffer) {
        return buffer.getFloat();
    }

    private static Double readFloat64WithoutType(BreezeBuffer buffer) {
        return buffer.getDouble();
    }

    private static String readStringWithoutType(BreezeBuffer buffer) throws BreezeException {
        try {
            return new String(readBytesWithoutType(buffer, true), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new BreezeException("UnsupportedEncoding UTF-8");
        }
    }

    private static byte[] readBytesWithoutType(BreezeBuffer buffer, boolean isZigzag) {
        int size = getAndCheckSize(buffer, isZigzag);
        if (size == 0) {
            return new byte[]{};
        } else {
            byte[] b = new byte[size];
            buffer.get(b);
            return b;
        }
    }

    private static <T, K> void readMapWithoutType(BreezeBuffer buffer, Map<T, K> map, Type keyType, Type valueType) throws BreezeException {

        int size = getAndCheckSize(buffer);
        if (size == 0) {
            return;
        }
        int startPos = buffer.position();
        int endPos = startPos + size;
        while (buffer.position() < endPos) {
            map.put((T) readObjectByType(buffer, keyType), (K) readObjectByType(buffer, valueType));
        }
        if (buffer.position() != endPos) {
            throw new RuntimeException("Breeze deserialize wrong map size, except: " + size + " actual: " + (buffer.position() - startPos));
        }
    }

    private static <T> void readCollectionWithoutType(BreezeBuffer buffer, Collection<T> collection, Type type) throws BreezeException {
        int size = getAndCheckSize(buffer);
        if (size == 0) {
            return;
        }
        int startPos = buffer.position();
        int endPos = startPos + size;
        while (buffer.position() < endPos) {
            collection.add((T) readObjectByType(buffer, type));
        }
        if (buffer.position() != endPos) {
            throw new RuntimeException("Breeze deserialize wrong array size, except: " + size + " actual: " + (buffer.position() - startPos));
        }
    }

    private static Message readMessageWithoutType(BreezeBuffer buffer, Class<? extends Message> clz) throws BreezeException {
        Message message;
        try {
            message = clz.newInstance();
        } catch (Exception e) {
            throw new BreezeException("create new default Message fail. Message must have a constructor without arguments. e:" + e.getMessage());
        }
        String name = readString(buffer);
        if (name != null && (name.equalsIgnoreCase(message.getName()) || name.equalsIgnoreCase(message.getAlias()))) {
            return message.readFromBuf(buffer);
        }
        throw new BreezeException("message name not correct. message clase:" + message.getClass().getName() + ", serialized name:" + name);
    }

    private static Schema readSchemaWithOutType(BreezeBuffer buffer) {
        // TODO
        return null;
    }

    public static int getAndCheckSize(BreezeBuffer buffer) {
        return getAndCheckSize(buffer, false);
    }

    public static int getAndCheckSize(BreezeBuffer buffer, boolean isZigzag) {
        int size;
        if (isZigzag) {
            size = buffer.getZigzag32();
        } else {
            size = buffer.getInt();
        }
        if (size > buffer.remaining()) {
            throw new RuntimeException("Breeze deserialize fail! buffer not enough!need size:" + size);
        }
        return size;
    }

    @FunctionalInterface
    public interface ReadField {
        void readIndexField(int index) throws BreezeException;
    }

}
