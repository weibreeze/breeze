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
import com.weibo.breeze.message.SchemaDesc;
import com.weibo.breeze.serializer.*;
import com.weibo.breeze.type.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.weibo.breeze.type.Types.*;

/**
 * @author zhanglei28
 * @date 2019/3/25.
 */
@SuppressWarnings("rawtypes")
public class Breeze {
    public static final String BREEZE_SERIALIZER_SUFFIX = "BreezeSerializer";
    private static final Logger logger = LoggerFactory.getLogger(Breeze.class);
    private static final ThreadLocal<Set<String>> GET_SERIALIZER_SET = ThreadLocal.withInitial(HashSet::new);
    private static final ConcurrentHashMap<String, Message> messageInstanceMap = new ConcurrentHashMap<>(128);
    private static final Serializer[] defaultSerializers = new Serializer[]{
            new DateSerializer(),
            new BigDecimalSerializer(),
            new TimestampSerializer(),
            new BigIntegerSerializer(),
    };
    private static final List<SerializerResolver> extResolver = new ArrayList<>();
    public static int MAX_ELEM_SIZE = 100000;
    private static SerializerFactory serializerFactory = new DefaultSerializerFactory();

    static {
        // register default serializers
        for (Serializer serializer : defaultSerializers) {
            try {
                registerSerializer(serializer);
            } catch (BreezeException e) {
                logger.warn("register default breeze serializer fail. serializer:{}, e:{}", serializer.getClass().getName(), e.getMessage());
            }
        }
        // add protobuf resolver if found
        try {
            SerializerResolver protobufResolver = (SerializerResolver) Class.forName("com.weibo.breeze.protobuf.ProtoBufResolver").newInstance();
            addResolver(protobufResolver);
        } catch (Exception ignore) {
        }
    }

    public static Serializer getSerializer(Class clz) {
        return getSerializerFactory().getSerializer(clz);
    }

    public static Serializer getSerializer(String name) {
        return getSerializerFactory().getSerializer(name);
    }

    public static void registerSerializer(Serializer serializer) throws BreezeException {
        getSerializerFactory().registerSerializer(serializer);
    }

    public static void registerSerializer(String name, Serializer serializer) throws BreezeException {
        getSerializerFactory().registerSerializer(name, serializer);
    }

    public static void registerSchema(SchemaDesc schemaDesc) throws BreezeException {
        for (Schema schema : schemaDesc.getSchemas()) {
            Breeze.registerSerializer(new CommonSerializer(schema));
        }
    }

    public static SerializerFactory getSerializerFactory() {
        return serializerFactory;
    }

    // for extension
    public static void setSerializerFactory(SerializerFactory serializerFactory) {
        Breeze.serializerFactory = serializerFactory;
    }

    public static void addResolver(SerializerResolver resolver) {
        if (resolver != null) {
            extResolver.add(resolver);
        }
    }

    public static Message getMessageInstance(String name) {
        Message message = messageInstanceMap.get(name);
        if (message != null && message != NoMessage.instance) {
            return message.defaultInstance();
        }
        if (message == null) {
            // TODO limit map size?
            try {
                Class clz = Class.forName(name, true, Thread.currentThread().getContextClassLoader());
                if (Message.class.isAssignableFrom(clz)) {
                    messageInstanceMap.put(name, (Message) clz.newInstance());
                    return messageInstanceMap.get(name).defaultInstance();
                }
            } catch (ReflectiveOperationException ignore) {
                messageInstanceMap.put(name, NoMessage.instance);
            }
        }
        return null;
    }

    public static Message putMessageInstance(String name, Message message) {
        return messageInstanceMap.put(name, message);
    }

    public static void withStaticField(boolean with) {
        CommonSerializer.WITH_STATIC_FIELD = with;
    }

    public static boolean withStaticField() {
        return CommonSerializer.WITH_STATIC_FIELD;
    }

    public static int getMaxWriteCount() {
        return BreezeWriter.MAX_WRITE_COUNT;
    }

    public static void setMaxWriteCount(int maxWriteCount) {
        BreezeWriter.MAX_WRITE_COUNT = maxWriteCount;
    }

    // preload all breeze schemas in every jar
    public static boolean preLoadSchemas() {
        return SchemaLoader.loadAllSchema();
    }

    public static String getCleanName(String name) {
        if (name.contains("$")) {
            name = name.replaceAll("\\$", "");
        }
        return name;
    }

    public static BreezeType getBreezeType(Class clz, String fieldName) throws BreezeException {
        try {
            return getBreezeType(clz.getDeclaredField(fieldName).getGenericType());
        } catch (NoSuchFieldException ignore) {
        }
        return null;
    }

    public static BreezeType getBreezeTypeByObject(Object object) throws BreezeException {
        if (object == null) {
            throw new BreezeException("can not get breeze type by null object");
        }
        if (object instanceof GenericMessage) { // Compatible with the name specified GenericMessage
            return new TypeMessage((Message) object);
        }
        return getBreezeType(object.getClass());
    }

    // unknown type will return null
    @SuppressWarnings("unchecked")
    public static BreezeType getBreezeType(Type type) throws BreezeException {
        Class clz;
        ParameterizedType pt = null;
        if (type instanceof Class) {
            clz = (Class) type;
        } else if (type instanceof ParameterizedType) {
            pt = (ParameterizedType) type;
            clz = (Class) pt.getRawType();
        } else {
            return null;
        }
        if (Message.class.isAssignableFrom(clz)) {
            return new TypeMessage(clz);
        }

        if (clz == String.class) {
            return TYPE_STRING;
        }

        if (clz == Integer.class || clz == int.class) {
            return TYPE_INT32;
        }

        if (Map.class.isAssignableFrom(clz)) {
            if (pt != null && pt.getActualTypeArguments().length == 2) {
                try {
                    return new TypePackedMap(pt.getActualTypeArguments()[0], pt.getActualTypeArguments()[1]);
                } catch (BreezeException e) {
                    return TYPE_MAP;
                }
            }
            if (BreezeWriter.IS_PACK) {
                return new TypePackedMap();
            }
            return TYPE_MAP;
        }

        if (Collection.class.isAssignableFrom(clz)) {
            if (pt != null && pt.getActualTypeArguments().length == 1) {
                try {
                    return new TypePackedArray(pt.getActualTypeArguments()[0]);
                } catch (BreezeException e) {
                    return TYPE_ARRAY;
                }
            }
            if (BreezeWriter.IS_PACK) {
                return new TypePackedArray();
            }
            return TYPE_ARRAY;
        }

        if (clz == Boolean.class || clz == boolean.class) {
            return TYPE_BOOL;
        }

        if (clz == Long.class || clz == long.class) {
            return TYPE_INT64;
        }

        if (clz == Float.class || clz == float.class) {
            return TYPE_FLOAT32;
        }

        if (clz == Double.class || clz == double.class) {
            return TYPE_FLOAT64;
        }

        if (clz == Byte.class || clz == byte.class) {
            return TYPE_BYTE;
        }

        if (clz == Short.class || clz == short.class) {
            return TYPE_INT16;
        }

        if (clz.isArray()) {
            if (clz.getComponentType() == byte.class) {
                return TYPE_BYTE_ARRAY;
            }
            return new TypePackedArray();
        }

        if (GET_SERIALIZER_SET.get().contains(clz.getName())) {
            // circular get serializer, will be replaced later
            return new TypePlaceHolder(clz);
        }
        try {
            GET_SERIALIZER_SET.get().add(clz.getName());
            Serializer serializer = Breeze.getSerializer(clz);
            if (serializer != null) {
                return new TypeMessage(serializer);
            }
            return null;
        } finally {
            GET_SERIALIZER_SET.get().remove(clz.getName());
        }
    }

    public interface SerializerResolver {
        Serializer getSerializer(Class<?> clz);
    }

    private static class NoMessage implements Message {
        static final NoMessage instance = new NoMessage();

        @Override
        public void writeToBuf(BreezeBuffer buffer) throws BreezeException {
        }

        @Override
        public Message readFromBuf(BreezeBuffer buffer) throws BreezeException {
            return null;
        }

        @Override
        public String messageName() {
            return null;
        }

        @Override
        public String messageAlias() {
            return null;
        }

        @Override
        public Message defaultInstance() {
            return null;
        }

        @Override
        public Schema schema() {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public static class DefaultSerializerFactory implements SerializerFactory {

        private final ConcurrentHashMap<String, Serializer> serializerMap = new ConcurrentHashMap<>(32);

        private DefaultSerializerFactory() {
        }

        public static Serializer getSerializerClassByName(String className) {
            try {
                Class serializerClass = Class.forName(getCleanName(className) + BREEZE_SERIALIZER_SUFFIX);
                if (Serializer.class.isAssignableFrom(serializerClass)) {
                    Serializer serializer = (Serializer) serializerClass.newInstance();
                    Breeze.registerSerializer(serializer);
                    return serializer;
                }
            } catch (Exception ignore) {
            }
            return null;
        }

        @Override
        public Serializer getSerializer(Class clz) {
            if (clz != null) {
                Serializer serializer;
                Class cur = clz;
                if (clz.isEnum()) {
                    try {
                        Class.forName(clz.getName()); // ensure the static block of enum message is executed
                    } catch (ClassNotFoundException ignore) {
                    }
                }
                while (cur != null && cur != Object.class) {
                    serializer = getSerializer(cur.getName());
                    if (serializer != null) {
                        return serializer;
                    }
                    // serializer by specified class name
                    serializer = getSerializerClassByName(cur.getName());
                    if (serializer != null) {
                        return serializer;
                    }

                    cur = cur.getSuperclass();
                }

                if (clz.isEnum()) {
                    try {
                        serializer = new EnumSerializer(clz);
                        registerSerializer(serializer);
                        return serializer;
                    } catch (BreezeException e) {
                        logger.warn("create enum serializer fail. clz:{}, e:{}", clz.getName(), e.getMessage());
                    }
                    return null;
                }
                for (SerializerResolver resolver : extResolver) {
                    serializer = resolver.getSerializer(clz);
                    if (serializer != null) {
                        try {
                            registerSerializer(serializer);
                            return serializer;
                        } catch (BreezeException e) {
                            logger.warn("register ext serializer fail. clz:{}, e:{}", clz.getName(), e.getMessage());
                        }
                    }
                }

                if (clz != Object.class && !clz.isInterface()) {
                    try {
                        // load from META-INF/breeze/${className}.breeze
                        Schema schema = SchemaLoader.loadSchema(clz.getName());
                        CommonSerializer commonSerializer;
                        if (schema != null) {
                            commonSerializer = new CommonSerializer(schema);
                        } else {
                            commonSerializer = new CommonSerializer(clz);
                        }
                        registerSerializer(commonSerializer);
                        return commonSerializer;
                    } catch (BreezeException e) {
                        logger.warn("create common serializer fail. clz:{}, e:{}", clz.getName(), e.getMessage());
                    }

                    Class[] interfaces = clz.getInterfaces();
                    for (Class interfaceClass : interfaces) {
                        serializer = getSerializer(interfaceClass.getName());
                        if (serializer != null) {
                            return serializer;
                        }
                    }
                }
            }
            return null;
        }

        @Override
        public Serializer getSerializer(String name) {
            return serializerMap.get(name);
        }

        @Override
        public Serializer removeSerializer(String name) {
            return serializerMap.remove(name);
        }

        @Override
        public void registerSerializer(Serializer serializer) throws BreezeException {
            if (serializer != null) {
                for (String name : serializer.getNames()) {
                    registerSerializer(name, serializer);
                }
            }
        }

        @Override
        public void registerSerializer(String name, Serializer serializer) throws BreezeException {
            if (serializer == null) {
                throw new BreezeException("serializer is null. name: " + name);
            }
            Serializer old = serializerMap.put(name, serializer);
            if (old != null) {
                logger.warn("DefaultSerializerFactory-serializer name {}: {} replaced by {}", name, old.getClass(), serializer.getClass());
            }
            logger.info("register breeze serializer:" + name);
        }

        @Override
        public Map<String, Serializer> getSerializers() {
            return Collections.unmodifiableMap(serializerMap);
        }
    }
}
