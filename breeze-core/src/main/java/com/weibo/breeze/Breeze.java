package com.weibo.breeze;

import com.weibo.breeze.message.Message;
import com.weibo.breeze.message.Schema;
import com.weibo.breeze.message.SchemaDesc;
import com.weibo.breeze.serializer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhanglei28 on 2019/3/25.
 */
public class Breeze {
    public static final String KEY_TYPE_SUFFIX = "KeyType";
    public static final String VALUE_TYPE_SUFFIX = "ValueType";
    public static final String BREEZE_SERIALIZER_SUFFIX = "BreezeSerializer";

    private static final Logger logger = LoggerFactory.getLogger(Breeze.class);
    private static SerializerFactory serializerFactory = new DefaultSerializerFactory();
    private static ConcurrentHashMap<String, Message> messageInstanceMap = new ConcurrentHashMap<>(128);
    private static Serializer[] defaultSerializers = new Serializer[]{
            new DateSerializer(),
            new BigDecimalSerializer(),
            new TimestampSerializer(),
            new BigIntegerSerializer(),
    };
    private static List<SerializerResolver> extResolver = new ArrayList<>();

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

    // for extionsion
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
            return message.getDefaultInstance();
        }
        if (message == null) {
            // TODO limit map size?
            try {
                Class clz = Class.forName(name, true, Thread.currentThread().getContextClassLoader());
                if (Message.class.isAssignableFrom(clz)) {
                    messageInstanceMap.put(name, (Message) clz.newInstance());
                    return messageInstanceMap.get(name).getDefaultInstance();
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

    public static void setMaxWriteCount(int maxWriteCount) {
        BreezeWriter.MAX_WRITE_COUNT = maxWriteCount;
    }

    public static int getMaxWriteCount() {
        return BreezeWriter.MAX_WRITE_COUNT;
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

    /**
     * add map or list field genericType into a type's map
     *
     * @param genericTypes generic type map to add
     * @param clz          target class to find out generic type
     * @param fieldName    target field name of target class
     */
    public static void addGenericType(Map<String, Type> genericTypes, Class clz, String fieldName) {
        try {
            Type type = clz.getDeclaredField(fieldName).getGenericType();
            if (type instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType) type;
                if (Map.class.isAssignableFrom((Class) pt.getRawType())) {
                    genericTypes.put(fieldName + KEY_TYPE_SUFFIX, pt.getActualTypeArguments()[0]);
                    genericTypes.put(fieldName + VALUE_TYPE_SUFFIX, pt.getActualTypeArguments()[1]);
                } else if (List.class.isAssignableFrom((Class) pt.getRawType())) {
                    genericTypes.put(fieldName + VALUE_TYPE_SUFFIX, pt.getActualTypeArguments()[0]);
                }
            }
        } catch (NoSuchFieldException ignore) {
        }
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
        public String getName() {
            return null;
        }

        @Override
        public String getAlias() {
            return null;
        }

        @Override
        public Message getDefaultInstance() {
            return null;
        }

        @Override
        public Schema getSchema() {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public static class DefaultSerializerFactory implements SerializerFactory {

        private DefaultSerializerFactory() {
        }

        private ConcurrentHashMap<String, Serializer> serializerMap = new ConcurrentHashMap<>(32);

        @Override
        public Serializer getSerializer(Class clz) {
            if (clz != null) {
                Serializer serializer;
                Class cur = clz;
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
            return serializerMap.get(getCleanName(name));
        }

        @Override
        public Serializer removeSerializer(String name) {
            return serializerMap.remove(getCleanName(name));
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
            name = getCleanName(name);
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
    }

    public interface SerializerResolver {
        Serializer getSerializer(Class<?> clz);
    }
}
