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

import com.weibo.breeze.message.GenericMessage;
import com.weibo.breeze.message.Message;
import com.weibo.breeze.message.Schema;
import com.weibo.breeze.serializer.CommonSerializer;
import com.weibo.breeze.serializer.Serializer;
import com.weibo.breeze.type.*;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

import static com.weibo.breeze.type.Types.*;

/**
 * @author zhanglei28
 * @date 2019/3/21.
 */
@SuppressWarnings("all")
public class BreezeReader {

    public static Boolean readBool(BreezeBuffer buffer) throws BreezeException {
        return TYPE_BOOL.read(buffer);
    }

    public static Byte readByte(BreezeBuffer buffer) throws BreezeException {
        return TYPE_BYTE.read(buffer);
    }

    public static Short readInt16(BreezeBuffer buffer) throws BreezeException {
        return TYPE_INT16.read(buffer);
    }

    public static Integer readInt32(BreezeBuffer buffer) throws BreezeException {
        return TYPE_INT32.read(buffer);
    }

    public static Long readInt64(BreezeBuffer buffer) throws BreezeException {
        return TYPE_INT64.read(buffer);
    }

    public static Float readFloat32(BreezeBuffer buffer) throws BreezeException {
        return TYPE_FLOAT32.read(buffer);
    }

    public static Double readFloat64(BreezeBuffer buffer) throws BreezeException {
        return TYPE_FLOAT64.read(buffer);
    }

    public static String readString(BreezeBuffer buffer) throws BreezeException {
        return TYPE_STRING.read(buffer);
    }

    public static byte[] readBytes(BreezeBuffer buffer) throws BreezeException {
        return TYPE_BYTE_ARRAY.read(buffer);
    }

    public static <T, K> void readMap(BreezeBuffer buffer, Map<T, K> map, Type keyType, Type valueType) throws BreezeException {
        byte type = buffer.get();
        if (type != MAP && type != PACKED_MAP) {
            throw new BreezeException("cannot read to map. type:" + type);
        }
        readMapWithoutType(buffer, map, keyType, valueType, type == PACKED_MAP);

    }

    public static <T> void readCollection(BreezeBuffer buffer, Collection<T> collection, Type valueType) throws BreezeException {
        byte type = buffer.get();
        if (type != ARRAY && type != PACKED_ARRAY) {
            throw new BreezeException("cannot read to collection. type:" + type);
        }
        int size = (int) buffer.getVarint();
        readCollectionWithoutType(buffer, collection, valueType, size, type == PACKED_ARRAY);
    }

    public static Message readMessage(BreezeBuffer buffer, Class<? extends Message> clz) throws BreezeException {
        byte type = buffer.get();
        if (type == NULL) {
            return null;
        }
        String name = readMessageName(buffer, type);
        return readMessageWithoutType(buffer, clz, name);
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
            index = (int) buffer.getVarint();
            readField.readIndexField(index);
        }
        if (buffer.position() != endPos) {
            throw new BreezeException("Breeze deserialize wrong message size, except: " + size + " actual: " + (buffer.position() - startPos));
        }
    }

    public static Schema readSchema(BreezeBuffer buffer, boolean withType) throws BreezeException {
        //TODO
        throw new BreezeException("todo: read schema");
    }

    public static void checkType(BreezeBuffer buffer, byte type) throws BreezeException {
        byte bufType = buffer.get();
        if (bufType != type) {
            throw new BreezeException("message type not correct. expect type:" + type + ", real type:" + bufType);
        }
    }

    public static <T> T readObject(BreezeBuffer buffer, Class<T> clz) throws BreezeException {
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

        byte bType = buffer.get();
        // string
        if (bType >= DIRECT_STRING_MIN_TYPE && bType <= STRING) {
            String string;
            if (bType == STRING) {
                string = TYPE_STRING.readString(buffer);
            } else {
                string = TYPE_STRING.readString(buffer, bType);
            }
            if (clz == String.class || clz == Object.class) {
                return string;
            }
            return adaptFromString(string, clz);
        }
        // int32
        if (bType >= DIRECT_INT32_MIN_TYPE && bType <= INT32) {
            Integer integer;
            if (bType == INT32) {
                integer = TYPE_INT32.readInt32(buffer);
            } else {
                integer = bType - INT32_ZERO;
            }
            if (clz == int.class || clz == Integer.class || clz == Object.class) {
                return integer;
            }
            return adaptFromNumber(integer, clz);
        }
        // int64
        if (bType >= DIRECT_INT64_MIN_TYPE && bType <= INT64) {
            Long aLong;
            if (bType == INT64) {
                aLong = TYPE_INT64.readInt64(buffer);
            } else {
                aLong = (long) (bType - INT64_ZERO);
            }
            if (clz == long.class || clz == Long.class || clz == Object.class) {
                return aLong;
            }
            return adaptFromNumber(aLong, clz);
        }

        // message
        if (bType >= MESSAGE && bType <= DIRECT_REF_MESSAGE_MAX_TYPE) {
            String name = readMessageName(buffer, bType);
            if (Message.class.isAssignableFrom(clz)) {
                return readMessageWithoutType(buffer, (Class<Message>) clz, name);
            }
            Serializer serializer;
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
                        Breeze.registerSerializer(name, serializer);
                    }
                }
                if (serializer != null) {
                    return serializer.readFromBuf(buffer);
                }
                try {
                    clz = Class.forName(name); // check if the specified class exists
                } catch (ClassNotFoundException ignore) {
                }
                if (clz == Object.class) { // the specified class not exists
                    GenericMessage genericMessage = new GenericMessage();
                    genericMessage.setName(name);
                    genericMessage.readFromBuf(buffer);
                    return genericMessage;
                }
            }
            serializer = Breeze.getSerializer(clz);
            if (serializer != null) {
                return serializer.readFromBuf(buffer);
            }
            throw new BreezeException("can not serialize message named " + name);
        }

        Object o = null;
        switch (bType) {
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
            case MAP:
            case PACKED_MAP:
                Type keyType = Object.class;
                Type valueType = Object.class;
                if (pt != null && pt.getActualTypeArguments().length == 2) {
                    keyType = pt.getActualTypeArguments()[0];
                    valueType = pt.getActualTypeArguments()[1];
                }
                if (clz.isAssignableFrom(HashMap.class)) {
                    // contain Object, Map
                    return readMapWithoutType(buffer, null, keyType, valueType, bType == PACKED_MAP);
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
                valueType = Object.class;
                if (pt != null && pt.getActualTypeArguments().length == 1) {
                    valueType = pt.getActualTypeArguments()[0];
                }
                int size = (int) buffer.getVarint();
                if (size > Breeze.MAX_ELEM_SIZE) {
                    throw new BreezeException("breeze array size over limit. size" + size);
                }
                if (clz.isArray()) {
                    List list = new ArrayList(size);
                    readCollectionWithoutType(buffer, list, clz.getComponentType(), size, bType == PACKED_ARRAY);
                    Object objects = Array.newInstance(clz.getComponentType(), list.size());
                    for (int i = 0; i < list.size(); i++) {
                        Array.set(objects, i, list.get(i));
                    }
                    return objects;
                }
                if (clz.isAssignableFrom(ArrayList.class)) {
                    List list = new ArrayList(size);
                    readCollectionWithoutType(buffer, list, valueType, size, bType == PACKED_ARRAY);
                    return list;
                }
                if (clz.isAssignableFrom(HashSet.class)) {
                    HashSet hs = new HashSet(TypeMap.calculateInitSize(size));
                    readCollectionWithoutType(buffer, hs, valueType, size, bType == PACKED_ARRAY);
                    return hs;
                }
                if (!clz.isInterface() && Collection.class.isAssignableFrom(clz)) {
                    Collection collection = null;
                    try {
                        collection = (Collection) clz.newInstance();
                    } catch (Exception ignore) {
                    }
                    if (collection != null) {
                        readCollectionWithoutType(buffer, collection, valueType, size, bType == PACKED_ARRAY);
                        return collection;
                    }
                }
                break;
            case NULL:
                return null;
            case FLOAT32:
                Float aFloat = TYPE_FLOAT32.readFloat32(buffer);
                if (clz == float.class || clz == Float.class || clz == Object.class) {
                    return aFloat;
                }
                o = adaptFromNumber(aFloat, clz);
                break;
            case FLOAT64:
                Double aDouble = TYPE_FLOAT64.readFloat64(buffer);
                if (clz == double.class || clz == Double.class || clz == Object.class) {
                    return aDouble;
                }
                o = adaptFromNumber(aDouble, clz);
                break;
            case INT16:
                Short aShort = TYPE_INT16.readInt16(buffer);
                if (clz == short.class || clz == Short.class || clz == Object.class) {
                    return aShort;
                }
                o = adaptFromNumber(aShort, clz);
                break;
            case BYTE:
                Byte aByte = TYPE_BYTE.readByte(buffer);
                if (clz == byte.class || clz == Byte.class || clz == Object.class) {
                    return aByte;
                }
                break;
            case BYTE_ARRAY:
                byte[] bytes = TYPE_BYTE_ARRAY.readBytes(buffer);
                if (clz == byte[].class || clz == Object.class) {
                    return bytes;
                }
                break;
            case SCHEMA:
                //TODO
            default:
                throw new BreezeException("Breeze not support " + bType + " with receiver type:" + clz);
        }
        if (o != null) {
            return o;
        }
        // check serializer at last
        Serializer serializer = Breeze.getSerializer(clz);
        if (serializer != null) {
            return serializer.readFromBuf(buffer);
        }
        throw new BreezeException("Breeze not support " + bType + " with receiver type:" + clz);
    }

    private static <T> T adaptFromString(String string, Class<T> clz) throws BreezeException {
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
        throw new BreezeException("can not convert string to class:" + clz);
    }

    public static <T> T adaptFromNumber(Number number, Class<T> clz) throws BreezeException {
        if ((clz == int.class || clz == Integer.class)) {
            return (T) Integer.valueOf(number.intValue());
        }
        if (clz == long.class || clz == Long.class) {
            return (T) Long.valueOf(number.longValue());
        }
        if (clz == short.class || clz == Short.class) {
            return (T) Short.valueOf(number.shortValue());
        }
        if (clz == float.class || clz == Float.class) {
            return (T) Float.valueOf(number.floatValue());
        }
        if (clz == double.class || clz == Double.class) {
            return (T) Double.valueOf(number.doubleValue());
        }
        throw new BreezeException("can not convert number to class:" + clz);
    }

    private static <T, K> Map<T, K> readMapWithoutType(BreezeBuffer buffer, Map<T, K> map, Type keyType, Type valueType, boolean isPacked) throws BreezeException {
        if (isPacked) {
            return new TypePackedMap().read(buffer, map, keyType, valueType, false);
        } else {
            return TYPE_MAP.read(buffer, map, keyType, valueType, false);
        }
    }

    private static <T> void readCollectionWithoutType(BreezeBuffer buffer, Collection<T> collection, Type type, int size, boolean isPacked) throws BreezeException {
        if (isPacked) {
            new TypePackedArray().readBySize(buffer, collection, type, size, true);
        } else {
            TYPE_ARRAY.readBySize(buffer, collection, type, size);
        }
    }

    private static Message readMessageWithoutType(BreezeBuffer buffer, Class<? extends Message> clz, String name) throws BreezeException {
        Message message;
        try {
            message = clz.newInstance();
        } catch (Exception e) {
            throw new BreezeException("create new default Message fail. Message must have a constructor without arguments. e:" + e.getMessage());
        }

        if (name != null && (name.equalsIgnoreCase(message.messageName()) || name.equalsIgnoreCase(message.messageAlias()))) {
            return message.readFromBuf(buffer);
        }
        throw new BreezeException("message name not correct. message clase:" + message.getClass().getName() + ", serialized name:" + name);
    }

    public static int getAndCheckSize(BreezeBuffer buffer) throws BreezeException {
        int size = buffer.getInt();
        if (size > buffer.remaining()) {
            throw new BreezeException("Breeze deserialize fail! buffer not enough!need size:" + size);
        }
        return size;
    }

    public static void skipType(BreezeBuffer buffer) throws BreezeException {
        byte type = buffer.get();
        if (type == MESSAGE) {
            buffer.getContext().putMessageType(TYPE_STRING.readString(buffer));
        } else if (type == REF_MESSAGE) {
            buffer.getVarint();
        }
    }

    public static BreezeType readBreezeType(BreezeBuffer buffer, Type type) throws BreezeException {
        byte bType = buffer.get();
        // message
        if (bType == MESSAGE || (bType >= REF_MESSAGE && bType <= DIRECT_REF_MESSAGE_MAX_TYPE)) {
            if (type == null) {
                type = Object.class;
            }
            Class clz;
            if (type instanceof Class) {
                clz = (Class) type;
            } else if (type instanceof ParameterizedType) {
                clz = (Class) ((ParameterizedType) type).getRawType();
            } else {
                throw new BreezeException("unknown read type :" + type);
            }
            String name = readMessageName(buffer, bType);
            if (clz == Object.class || clz.isInterface()) {
                Message message = Breeze.getMessageInstance(name);
                if (message != null) {
                    return new TypeMessage(message);
                }
                Serializer serializer = Breeze.getSerializer(name); // direct register serializer
                if (serializer == null) {// has breeze schema file
                    Schema schema = SchemaLoader.loadSchema(name);
                    if (schema != null) {
                        serializer = new CommonSerializer(schema);
                        Breeze.registerSerializer(name, serializer);
                    }
                }
                if (serializer != null) {
                    return new TypeMessage(serializer);
                }
                try {
                    clz = Class.forName(name); // check if the specified class exists
                } catch (ClassNotFoundException ignore) {
                }
                if (clz == Object.class) {
                    GenericMessage genericMessage = new GenericMessage();
                    genericMessage.setName(name);
                    return new TypeMessage(genericMessage);
                }
            }
            return new TypeMessage(clz);
        }
        switch (bType) {
            case STRING:
                return TYPE_STRING;
            case INT32:
                return TYPE_INT32;
            case TRUE:
                return TYPE_BOOL;
            case INT64:
                return TYPE_INT64;
            case MAP:
                return TYPE_MAP;
            case PACKED_MAP:
                return new TypePackedMap();
            case ARRAY:
                return TYPE_ARRAY;
            case PACKED_ARRAY:
                return new TypePackedArray();
            case FLOAT32:
                return TYPE_FLOAT32;
            case FLOAT64:
                return TYPE_FLOAT64;
            case INT16:
                return TYPE_INT16;
            case BYTE:
                return TYPE_BYTE;
            case BYTE_ARRAY:
                return TYPE_BYTE_ARRAY;
        }
        return null;
    }

    public static String readMessageName(BreezeBuffer buffer, byte typeByte) throws BreezeException {
        String name = null;
        int index = 0;
        if (typeByte == MESSAGE) {
            name = TYPE_STRING.readString(buffer);
            buffer.getContext().putMessageType(name);
        } else if (typeByte == REF_MESSAGE) {
            index = (int) buffer.getVarint();
        } else if (typeByte > REF_MESSAGE && typeByte <= DIRECT_REF_MESSAGE_MAX_TYPE) {
            index = typeByte - REF_MESSAGE;
        } else {
            throw new BreezeException("message type not correct. type:" + typeByte);
        }
        if (index > 0) {
            name = buffer.getContext().getMessageTypeName(index);
            if (name == null) {
                throw new BreezeException("wrong breeze message ref. index:" + index);
            }
        }
        return name;
    }

    @FunctionalInterface
    public interface ReadField {
        void readIndexField(int index) throws BreezeException;
    }
}
