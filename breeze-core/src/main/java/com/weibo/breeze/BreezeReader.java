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
import java.nio.charset.Charset;
import java.util.*;

import static com.weibo.breeze.BreezeType.*;

/**
 * Created by zhanglei28 on 2019/3/21.
 */
@SuppressWarnings("all")
public class BreezeReader {
    static Charset charset = Charset.forName("UTF-8");

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
        return readByte(buffer, true);
    }

    public static Short readInt16(BreezeBuffer buffer) throws BreezeException {
        return readInt16(buffer, true);
    }

    public static Integer readInt32(BreezeBuffer buffer) throws BreezeException {
        return readInt32(buffer, true);
    }

    public static Long readInt64(BreezeBuffer buffer) throws BreezeException {
        return readInt64(buffer, true);
    }

    public static Float readFloat32(BreezeBuffer buffer) throws BreezeException {
        return readFloat32(buffer, true);
    }

    public static Double readFloat64(BreezeBuffer buffer) throws BreezeException {
        return readFloat64(buffer, true);
    }

    public static String readString(BreezeBuffer buffer) throws BreezeException {
        return readString(buffer, true);
    }

    public static byte[] readBytes(BreezeBuffer buffer) throws BreezeException {
        return readBytes(buffer, true);
    }

    public static Byte readByte(BreezeBuffer buffer, boolean withType) throws BreezeException {
        if (withType) {
            checkType(buffer, BYTE);
        }
        return buffer.get();
    }

    public static Short readInt16(BreezeBuffer buffer, boolean withType) throws BreezeException {
        if (withType) {
            checkType(buffer, INT16);
        }
        return buffer.getShort();
    }

    public static Integer readInt32(BreezeBuffer buffer, boolean withType) throws BreezeException {
        if (withType) {
            checkType(buffer, INT32);
        }
        return buffer.getZigzag32();
    }

    public static Long readInt64(BreezeBuffer buffer, boolean withType) throws BreezeException {
        if (withType) {
            checkType(buffer, INT64);
        }
        return buffer.getZigzag64();
    }

    public static Float readFloat32(BreezeBuffer buffer, boolean withType) throws BreezeException {
        if (withType) {
            checkType(buffer, FLOAT32);
        }
        return buffer.getFloat();
    }

    public static Double readFloat64(BreezeBuffer buffer, boolean withType) throws BreezeException {
        if (withType) {
            checkType(buffer, FLOAT64);
        }
        return buffer.getDouble();
    }

    public static String readString(BreezeBuffer buffer, boolean withType) throws BreezeException {
        if (withType) {
            checkType(buffer, STRING);
        }
        return buffer.getUTF8();
//        try {
//            return new String(readBytesWithoutType(buffer, true), "UTF-8");
//        } catch (UnsupportedEncodingException e) {
//            throw new BreezeException("decode utf8 fail. err:" + e.getMessage());
//        }
    }

    public static byte[] readBytes(BreezeBuffer buffer, boolean withType) throws BreezeException {
        if (withType) {
            checkType(buffer, BYTE_ARRAY);
        }
        return readBytesWithoutType(buffer);
    }

    public static Map readMap(BreezeBuffer buffer) throws BreezeException {
        Map result = new HashMap<>();
        readMap(buffer, result, Object.class, Object.class);
        return result;
    }

    // with generic type
    public static <T, K> void readMap(BreezeBuffer buffer, Map<T, K> map, Type keyType, Type valueType) throws BreezeException {
        readMap(buffer, map, keyType, valueType, true, true);
    }

    public static <T, K> void readMap(BreezeBuffer buffer, Map<T, K> map, Type keyType, Type valueType, boolean isPacked, boolean withType) throws BreezeException {
        if (withType) {
            checkType(buffer, isPacked ? PACKED_MAP : MAP);
        }
        readMapWithoutType(buffer, map, keyType, valueType, isPacked);
    }

    // with generic type
    public static <T> void readCollection(BreezeBuffer buffer, Collection<T> collection, Type type) throws BreezeException {
        readCollection(buffer, collection, type, true, true);
    }

    // with generic type
    public static <T> void readCollection(BreezeBuffer buffer, Collection<T> collection, Type type, boolean isPacked, boolean withType) throws BreezeException {
        if (withType) {
            checkType(buffer, isPacked ? PACKED_ARRAY : ARRAY);
        }
        readCollectionWithoutType(buffer, collection, type, isPacked);
    }

    public static Message readMessage(BreezeBuffer buffer, Class<? extends Message> clz) throws BreezeException {
        BreezeType breezeType = readBreezeType(buffer);
        if (breezeType.type != MESSAGE) {
            throw new BreezeException("message type not correct. type:" + breezeType.type);
        }
        return readMessageWithoutType(buffer, clz, breezeType.messageName);
    }

    public static void readMessage(BreezeBuffer buffer, ReadField readField) throws BreezeException {
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
            throw new BreezeException("Breeze deserialize wrong map size, except: " + size + " actual: " + (buffer.position() - startPos));
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
            throw new BreezeException("Breeze deserialize wrong map size, except: " + size + " actual: " + (buffer.position() - startPos));
        }
    }

    public static Schema readSchema(BreezeBuffer buffer, boolean withType) throws BreezeException {
        if (withType) {
            checkType(buffer, SCHEMA);
        }
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
        return (T) readObjectByType(buffer, clz, null);
    }

    public static Object readObjectByType(BreezeBuffer buffer, Type type, BreezeType breezeType) throws BreezeException {
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
        byte bType;
        if (breezeType == null) {
            breezeType = readBreezeType(buffer);
            bType = breezeType.type;
        } else {
            if (breezeType.type == TRUE) { // bool type
                bType = buffer.get();
            } else {
                bType = breezeType.type;
            }
        }
        switch (bType) {
            case NULL:
                return null;
            case STRING:
                String string = readString(buffer, false);
                if (clz == String.class || clz == Object.class) {
                    return string;
                }
                o = adaptFromString(string, clz);
                break;
            case BYTE_ARRAY:
                byte[] bytes = readBytesWithoutType(buffer);
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
                Byte aByte = readByte(buffer, false);
                if (clz == byte.class || clz == Byte.class || clz == Object.class) {
                    return aByte;
                }
                break;
            case INT16:
                Short aShort = readInt16(buffer, false);
                if (clz == short.class || clz == Short.class || clz == Object.class) {
                    return aShort;
                }
                o = adaptFromNumber(aShort, clz);
                break;
            case INT32:
                Integer integer = readInt32(buffer, false);
                if (clz == int.class || clz == Integer.class || clz == Object.class) {
                    return integer;
                }
                o = adaptFromNumber(integer, clz);
                break;
            case INT64:
                Long aLong = readInt64(buffer, false);
                if (clz == long.class || clz == Long.class || clz == Object.class) {
                    return aLong;
                }
                o = adaptFromNumber(aLong, clz);
                break;
            case FLOAT32:
                Float aFloat = readFloat32(buffer, false);
                if (clz == float.class || clz == Float.class || clz == Object.class) {
                    return aFloat;
                }
                o = adaptFromNumber(aFloat, clz);
                break;
            case FLOAT64:
                Double aDouble = readFloat64(buffer, false);
                if (clz == double.class || clz == Double.class || clz == Object.class) {
                    return aDouble;
                }
                o = adaptFromNumber(aDouble, clz);
                break;
            case MAP:
            case PACKED_MAP:
                if (pt != null && pt.getActualTypeArguments().length == 2) {
                    keyType = pt.getActualTypeArguments()[0];
                    valueType = pt.getActualTypeArguments()[1];
                }
                if (clz.isAssignableFrom(HashMap.class)) {
                    // contain Object, Map
                    Map map = new HashMap<>();
                    readMapWithoutType(buffer, map, keyType, valueType, bType == PACKED_MAP);
                    return map;
                }
                if (!clz.isInterface() && Map.class.isAssignableFrom(clz)) {
                    Map map = null;
                    try {
                        map = (Map) clz.newInstance();
                    } catch (Exception ignore) {
                    }
                    if (map != null) {
                        readMapWithoutType(buffer, map, keyType, valueType, bType == PACKED_MAP);
                        return map;
                    }
                }
                break;
            case ARRAY:
            case PACKED_ARRAY:
                if (pt != null && pt.getActualTypeArguments().length == 1) {
                    valueType = pt.getActualTypeArguments()[0];
                }
                if (clz.isArray()) {
                    List list = new ArrayList();
                    readCollectionWithoutType(buffer, list, clz.getComponentType(), bType == PACKED_ARRAY);
                    Object objects = Array.newInstance(clz.getComponentType(), list.size());
                    for (int i = 0; i < list.size(); i++) {
                        Array.set(objects, i, list.get(i));
                    }
                    return objects;
                }
                if (clz.isAssignableFrom(ArrayList.class)) {
                    List list = new ArrayList();
                    readCollectionWithoutType(buffer, list, valueType, bType == PACKED_ARRAY);
                    return list;
                }
                if (clz.isAssignableFrom(HashSet.class)) {
                    HashSet hs = new HashSet();
                    readCollectionWithoutType(buffer, hs, valueType, bType == PACKED_ARRAY);
                    return hs;
                }
                if (!clz.isInterface() && Collection.class.isAssignableFrom(clz)) {
                    Collection collection = null;
                    try {
                        collection = (Collection) clz.newInstance();
                    } catch (Exception ignore) {
                    }
                    if (collection != null) {
                        readCollectionWithoutType(buffer, collection, valueType, bType == PACKED_ARRAY);
                        return collection;
                    }
                }
                break;
            case MESSAGE:
                String name;
                if (breezeType != null) {
                    name = breezeType.messageName;
                } else {
                    name = readString(buffer, false);
                    buffer.getContext().putMessageType(name);
                }
                if (Message.class.isAssignableFrom(clz)) {
                    return readMessageWithoutType(buffer, (Class<Message>) clz, name);
                }
                if (clz == Object.class || clz.isInterface()) {
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
                        return serializer.readFromBuf(buffer);
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
                throw new BreezeException("Breeze not support " + breezeType + " with receiver type:" + clz);
        }
        if (o != null) {
            return o;
        }
        //
        serializer = Breeze.getSerializer(clz);
        if (serializer != null) {
            return serializer.readFromBuf(buffer);
        }
        throw new BreezeException("Breeze not support " + breezeType + " with receiver type:" + clz);
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

    private static byte[] readBytesWithoutType(BreezeBuffer buffer) throws BreezeException {
        int size = getAndCheckSize(buffer);
        if (size == 0) {
            return new byte[]{};
        } else {
            byte[] b = new byte[size];
            buffer.get(b);
            return b;
        }
    }

    private static <T, K> void readMapWithoutType(BreezeBuffer buffer, Map<T, K> map, Type keyType, Type valueType, boolean isPacked) throws BreezeException {

        int size = getAndCheckSize(buffer);
        if (size == 0) {
            return;
        }
        int startPos = buffer.position();
        int endPos = startPos + size;

        BreezeType breezeKeyType = null;
        BreezeType breezeValueType = null;
        if (isPacked) {
            breezeKeyType = readBreezeType(buffer);
            breezeValueType = readBreezeType(buffer);
        }
        while (buffer.position() < endPos) {
            map.put((T) readObjectByType(buffer, keyType, breezeKeyType), (K) readObjectByType(buffer, valueType, breezeValueType));
        }
        if (buffer.position() != endPos) {
            throw new BreezeException("Breeze deserialize wrong map size, except: " + size + " actual: " + (buffer.position() - startPos));
        }
    }

    private static <T> void readCollectionWithoutType(BreezeBuffer buffer, Collection<T> collection, Type type, boolean isPacked) throws BreezeException {
        int size = getAndCheckSize(buffer);
        if (size == 0) {
            return;
        }
        int startPos = buffer.position();
        int endPos = startPos + size;
        BreezeType breezeType = null;
        if (isPacked) {
            breezeType = readBreezeType(buffer);
        }
        while (buffer.position() < endPos) {
            collection.add((T) readObjectByType(buffer, type, breezeType));
        }
        if (buffer.position() != endPos) {
            throw new BreezeException("Breeze deserialize wrong array size, except: " + size + " actual: " + (buffer.position() - startPos));
        }
    }

    private static Message readMessageWithoutType(BreezeBuffer buffer, Class<? extends Message> clz, String name) throws BreezeException {
        Message message;
        try {
            message = clz.newInstance();
        } catch (Exception e) {
            throw new BreezeException("create new default Message fail. Message must have a constructor without arguments. e:" + e.getMessage());
        }

        if (name != null && (name.equalsIgnoreCase(message.getName()) || name.equalsIgnoreCase(message.getAlias()))) {
            return message.readFromBuf(buffer);
        }
        throw new BreezeException("message name not correct. message clase:" + message.getClass().getName() + ", serialized name:" + name);
    }

    private static Schema readSchemaWithOutType(BreezeBuffer buffer) {
        // TODO
        return null;
    }

   public static int getAndCheckSize(BreezeBuffer buffer) throws BreezeException {
        int size = buffer.getInt();
        if (size > buffer.remaining()) {
            throw new BreezeException("Breeze deserialize fail! buffer not enough!need size:" + size);
        }
        return size;
    }

    @FunctionalInterface
    public interface ReadField {
        void readIndexField(int index) throws BreezeException;
    }

    private static BreezeType readBreezeType(BreezeBuffer buffer) throws BreezeException {
        byte bType = buffer.get();
        BreezeType breezeType = new BreezeType(bType, null);
        int index = 0;
        // message ref
        if (bType == MESSAGE) {
            breezeType.messageName = readString(buffer, false);
            buffer.getContext().putMessageType(breezeType.messageName);
        } else if (bType == TYPE_REF_MESSAGE) {
            index = buffer.getZigzag32();
        } else if (bType > TYPE_REF_MESSAGE && bType <= TYPE_REF_MESSAGE + MAX_DIRECT_MESSAGE_TYPE_REF) {
            index = bType - TYPE_REF_MESSAGE;
        }
        if (index > 0) {
            String name = buffer.getContext().getMessageTypeName(index);
            if (name == null) {
                throw new BreezeException("wrong breeze message ref. index:" + index);
            }
            breezeType.type = MESSAGE;
            breezeType.messageName = name;
        }
        return breezeType;
    }

}
