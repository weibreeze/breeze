package com.weibo.breeze;

import com.weibo.breeze.message.Message;
import com.weibo.breeze.message.Schema;
import com.weibo.breeze.message.SchemaDesc;
import com.weibo.breeze.serializer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhanglei28 on 2019/3/25.
 */
public class Breeze {
    private static final Logger logger = LoggerFactory.getLogger(Breeze.class);
    private static SerializerFactory serializerFactory = new DefaultSerializerFactory();
    private static ConcurrentHashMap<String, Message> messageInstanceMap = new ConcurrentHashMap<>(128);
    private static Serializer[] defaultSerializers = new Serializer[]{
            new DateSerializer(),
            new BigDecimalSerializer(),
            new TimestampSerializer(),
    };
    private static List<SerializerResolver> extResolver = new ArrayList<>();

    static {
        // register default serializers
        for (Serializer serializer : defaultSerializers) {
            try {
                registerSerializer(serializer);
            } catch (BreezeException e) {
                logger.warn("register default serializer fail. serializer:{}, e:{}", serializer.getClass().getName(), e.getMessage());
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
                    return messageInstanceMap.get(name);
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
        BreezeWriter.MAX_WRITE_COUNTE = maxWriteCount;
    }

    public static int getMaxWriteCount() {
        return BreezeWriter.MAX_WRITE_COUNTE;
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
                        CommonSerializer commonSerializer = new CommonSerializer(clz);
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
                logger.warn("DefaultSerializerFactory-serializer name {}: {} replaced by {}", new Object[]{name, old.getClass(), serializer.getClass()});
            }
            logger.info("register breeze serializer:" + name);
        }

        @Override
        public Map<String, Serializer> getSerializers() {
            return Collections.unmodifiableMap(serializerMap);
        }
    }

    public static interface SerializerResolver {
        Serializer getSerializer(Class clz);
    }
}
