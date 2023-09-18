/*
 *
 *   Copyright 2023 Weibo, Inc.
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
import com.weibo.breeze.serializer.CommonSerializer;
import com.weibo.breeze.serializer.EnumSerializer;
import com.weibo.breeze.serializer.Serializer;

import java.lang.reflect.*;
import java.util.*;

/**
 * Assist in generating java class serializer, schema, breeze file
 *
 * @author zhanglei28
 * @date 2023/9/13.
 */
public class BreezeUtil {
    // config keys
    public static final String WITH_STATIC_FIELD_KEY = "withStaticField";

    private static final Set<Class<?>> NOT_PROCESS_CLASS = new HashSet<>(Arrays.asList(
            Boolean.class, Byte.class, Character.class, Short.class, Integer.class, Long.class,
            Double.class, Float.class, Object.class, String.class, Void.TYPE
    ));
    // default values
    public static int DEFAULT_MAX_FIELD_SIZE = 1000;


    public static String generateBreezeSerializerContent(Class<?> clazz) {
        throw new RuntimeException();
    }

    public static Map<Class<?>, GenerateClassResult> generateSchema(Class<?> clz) {
        return generateSchema(clz, new HashMap<>());
    }

    public static Map<Class<?>, GenerateClassResult> generateSchema(Class<?> clz, Map<String, String> configs) {
        Map<Class<?>, GenerateClassResult> result = new HashMap<>();
        Map<String, Class<?>> needProcessClasses = new HashMap<>();
        findClassNeedProcess(clz, needProcessClasses);
        Map<String, Class<?>> willBeProcessedClasses = new HashMap<>(needProcessClasses);
        for (Map.Entry<String, Class<?>> entry : needProcessClasses.entrySet()) {
            try {
                generateClassSchema0(entry.getValue(), configs, willBeProcessedClasses, result);
            } catch (BreezeException e) {
                System.out.println("[WARN] BreezeUtil: generate class schema fail. class:" + entry.getKey() + ", error:" + e.getMessage());
            }
        }
        return result;
    }

    public static void findClassNeedProcess(Class<?> clz, Map<String, Class<?>> needProcessClasses) {
        Map<String, Class<?>> willBeProcessedClasses = new HashMap<>();
        if (clz.isInterface()) {
            for (Method method : clz.getMethods()) {
                for (Type param : method.getGenericParameterTypes()) {
                    needProcess(param, needProcessClasses, willBeProcessedClasses);
                }
                needProcess(method.getGenericReturnType(), needProcessClasses, willBeProcessedClasses);
            }
        } else {
            needProcess(clz, needProcessClasses, willBeProcessedClasses);
        }
    }

    /**
     * Generate breeze schema for specific classes
     * For internal use only
     *
     * @param clz                    class to generate breeze schema
     * @param configs                generate configs
     * @param willBeProcessedClasses will be processed classes. These classes will not be processed as associated classes
     * @param result                 generate result. it contains classes that were not successfully generated
     * @throws BreezeException throw when generate fail
     */
    public static void generateClassSchema0(Class<?> clz, Map<String, String> configs, Map<String, Class<?>> willBeProcessedClasses, Map<Class<?>, GenerateClassResult> result) throws BreezeException {
        if (configs == null) {
            throw new BreezeException("config map is null");
        }
        if (result == null) {
            throw new BreezeException("result map is null");
        }
        if (willBeProcessedClasses == null) {
            throw new BreezeException("willBeProcessedClasses map is null");
        }
        GenerateClassResult generateClassResult = new GenerateClassResult(clz);
        result.put(clz, generateClassResult);
        if (clz.isEnum()) {
            generateClassResult.schema = buildEnumSchema(clz, configs, willBeProcessedClasses, result);
            generateClassResult.success = true;
            return;
        }
        String name = getCleanName(clz);
        Schema schema = Schema.newSchema(name);
        if (clz.getName().contains("$")) {
            schema.setJavaName(clz.getName());
        }
        int index = 1;
        int round = 1;
        Method[] methods = clz.getMethods();
        Field[] fields;
        Set<String> addedFields = new HashSet<>();
        Class<?> curClz = clz;
        do {
            fields = curClz.getDeclaredFields();
            for (Field field : fields) {
                if (checkField(field, "true".equals(configs.get(WITH_STATIC_FIELD_KEY)))) {
                    if (Modifier.isPublic(field.getModifiers())) {
                        addField(schema, field, index++, addedFields, configs, willBeProcessedClasses, result);
                    } else if (methods.length > 0) {
                        for (Method method : methods) {// field with getter method
                            if (method.getName().equalsIgnoreCase("get" + field.getName())
                                    || ((field.getType() == boolean.class)
                                    && method.getName().equalsIgnoreCase("is" + field.getName()))) {
                                addField(schema, field, index++, addedFields, configs, willBeProcessedClasses, result);
                                break;
                            }
                        }
                    }
                }
            }
            if (curClz.getSuperclass() != Object.class && fields.length > DEFAULT_MAX_FIELD_SIZE) {
                throw new BreezeException("field size over limit. class:" + curClz.getName() + ", field size:" + fields.length);
            }
            curClz = curClz.getSuperclass();
            index = round * DEFAULT_MAX_FIELD_SIZE + 1;
            round++;
        } while (curClz != null && curClz != Object.class);
        generateClassResult.schema = schema;
        generateClassResult.success = true;
    }

    private static Schema buildEnumSchema(Class<?> clz, Map<String, String> configs, Map<String, Class<?>> willBeProcessedClasses, Map<Class<?>, GenerateClassResult> result) throws BreezeException {
        String name = getCleanName(clz);
        Schema schema = Schema.newSchema(name);
        if (clz.getName().contains("$")) {
            schema.setJavaName(clz.getName());
        }
        List<Field> targetFields = new ArrayList<>();
        for (Field field : clz.getDeclaredFields()) {
            int modifier = field.getModifiers();
            if (Modifier.isPrivate(modifier) && !Modifier.isStatic(modifier) && !"$VALUES".equals(field.getName())) {
                field.setAccessible(true);
                targetFields.add(field);
            }
        }
        Object[] enums = clz.getEnumConstants();
        if (targetFields.isEmpty()) {
            schema.setEnum(true);
            for (Object obj : enums) {
                schema.addEnumValue(((Enum<?>) obj).ordinal(), ((Enum<?>) obj).name());
            }
        } else if (targetFields.size() == 1 && (targetFields.get(0).getType() == int.class || targetFields.get(0).getType() == Integer.class)) {
            schema.setEnum(true);
            for (Object obj : enums) {
                try {
                    schema.addEnumValue((Integer) targetFields.get(0).get(obj), ((Enum<?>) obj).name());
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        } else { // as message
            schema.putField(1, "enumValue", "string"); // add enum name
            Set<String> addedFields = new HashSet<>();
            int index = 2;
            for (Field field : targetFields) {
                addField(schema, field, index++, addedFields, configs, willBeProcessedClasses, result);
            }
        }
        return schema;
    }

    public static String getCleanName(Class<?> clz) {
        String name = clz.getName();
        if (name.contains("$")) {
            name = name.replaceAll("\\$", "");
        }
        return name;
    }

    private static boolean checkField(Field field, boolean withStaticField) {
        return !Modifier.isFinal(field.getModifiers())
                && (withStaticField || !Modifier.isStatic(field.getModifiers()));
    }

    private static void addField(Schema schema, Field field, int index, Set<String> addedFields, Map<String, String> configs, Map<String, Class<?>> willBeProcessedClasses, Map<Class<?>, GenerateClassResult> result) {
        if (!addedFields.contains(field.getName())) {// skip duplicate filed, such as super class field.
            Map<String, Class<?>> associateClasses = new HashMap<>();
            needProcess(field.getGenericType(), associateClasses, willBeProcessedClasses);
            willBeProcessedClasses.putAll(associateClasses);
            for (Class<?> clz : associateClasses.values()) {
                try {
                    generateClassSchema0(clz, configs, willBeProcessedClasses, result);
                } catch (BreezeException e) {
                    System.out.println("[WARN] BreezeUtil: generate field associate schema fail. base class:" + schema.getName() + ", field name:" + field.getName() + ", field index:" + index + ", field associate class:" + clz + "，error: " + e.getMessage());
                }
            }
            try {
                schema.putField(index, field.getName(), getBreezeType(field.getGenericType(), schema.getName().substring(0, schema.getName().lastIndexOf("."))));
                addedFields.add(field.getName());
            } catch (BreezeException e) {
                System.out.println("[WARN] BreezeUtil: add schema field fail. class:" + schema.getName() + ", field name:" + field.getName() + ", field index:" + index + "， error: " + e.getMessage());
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void needProcess(Type type, Map<String, Class<?>> needProcessClasses, Map<String, Class<?>> willBeProcessedClasses) {
        if (type == null) {
            return;
        }
        Class<?> clz;
        if (type instanceof Class) {
            clz = (Class<?>) type;
        } else if (type instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType) type;
            clz = (Class<?>) pt.getRawType();
            for (Type tp : pt.getActualTypeArguments()) {// check all generic type
                needProcess(tp, needProcessClasses, willBeProcessedClasses);
            }
        } else {
            return;
        }
        // common type
        if (NOT_PROCESS_CLASS.contains(clz) || clz.isInterface() || clz.isPrimitive()
                || Map.class.isAssignableFrom(clz) || List.class.isAssignableFrom(clz)
                || Set.class.isAssignableFrom(clz)) {
            return;
        }
        if (clz.isArray()) { // check array component type
            needProcess(clz.getComponentType(), needProcessClasses, willBeProcessedClasses);
            return;
        }

        String name = getCleanName(clz);
        if (willBeProcessedClasses.containsKey(name) || needProcessClasses.containsKey(name)) {// already add to process.
            return;
        }
        //breeze
        if (Message.class.isAssignableFrom(clz)) {
            return;
        }
        // already has custom serializer
        Serializer<?> serializer = Breeze.DefaultSerializerFactory.getSerializerClassByName(name);
        if (serializer != null) {
            System.out.println("[INFO] BreezeUtil:" + name + " already has custom serializer " + serializer.getClass().getName());
            return;
        }
        if (clz.isEnum()) {
            try {
                Class.forName(clz.getName()); // ensure the static block of enum message is executed
            } catch (ClassNotFoundException ignore) {
            }
        }
        serializer = Breeze.getSerializer(name);
        if (serializer != null && !(serializer instanceof CommonSerializer) && !(serializer instanceof EnumSerializer)) {
            System.out.println("[INFO] BreezeUtil:" + name + " already has custom serializer " + serializer.getClass().getName());
            return;
        }
        if (clz.getSuperclass() != null) {
            String superClz = clz.getSuperclass().getName();
            //protobuf
            if ("com.google.protobuf.GeneratedMessage".equals(superClz) || "com.google.protobuf.GeneratedMessageV3".equals(superClz)) {
                System.out.println("[INFO] BreezeUtil: skip protobuf message " + clz.getName());
                return;
            }
        }
        // check weather can generate or not
        if (!clz.isEnum()) {
            try {
                new CommonSerializer(clz);
            } catch (BreezeException be) {
                System.out.println("[WARN] BreezeUtil: " + clz.getName() + " can not create commonSerializer. info:" + be.getMessage());
                return;
            }
        }
        needProcessClasses.put(name, clz);
    }

    private static String getBreezeType(Type type, String packageName) throws BreezeException {
        if (type == Object.class) {
            throw new BreezeException("can not support Object.class as breeze type.");
        }
        ParameterizedType pt = null;
        Class<?> clz;
        if (type instanceof Class) {
            clz = (Class<?>) type;
        } else if (type instanceof ParameterizedType) {
            pt = (ParameterizedType) type;
            clz = (Class<?>) pt.getRawType();
        } else {
            throw new BreezeException("unsupported type :" + type);
        }
        if (clz == String.class || clz == char.class || clz == Character.class || clz == void.class) {
            return "string";
        }

        if (clz == Byte.class || clz == byte.class) {
            return "byte";
        }

        if (clz == Boolean.class || clz == boolean.class) {
            return "bool";
        }

        if (clz == Short.class || clz == short.class) {
            return "int16";
        }

        if (clz == Integer.class || clz == int.class) {
            return "int32";
        }

        if (clz == Long.class || clz == long.class) {
            return "int64";
        }

        if (clz == Float.class || clz == float.class) {
            return "float32";
        }

        if (clz == Double.class || clz == double.class) {
            return "float64";
        }

        if (Map.class.isAssignableFrom(clz)) {
            if (pt == null || pt.getActualTypeArguments().length != 2) {
                throw new BreezeException("class must has two argument generic type when the class is a subclass of map. type:" + type);
            }
            return "map<" + getBreezeType(pt.getActualTypeArguments()[0], packageName) + ", " + getBreezeType(pt.getActualTypeArguments()[1], packageName) + ">";
        }

        if (clz.isArray()) {
            if (clz.getComponentType() == byte.class) {
                return "bytes";
            }
            return "array<" + getBreezeType(clz.getComponentType(), packageName) + ">";
        }

        if (Collection.class.isAssignableFrom(clz)) {
            if (pt == null || pt.getActualTypeArguments().length != 1) {
                throw new BreezeException("class must has a argument generic type when the class is a subclass of Collection. type:" + type);
            }
            return "array<" + getBreezeType(pt.getActualTypeArguments()[0], packageName) + ">";
        }
        // as message
        String name = getCleanName(clz);
        if (name.startsWith(packageName + ".") && !name.substring(packageName.length() + 1).contains(".")) { // same package
            return name.substring(packageName.length() + 1);
        }
        return name;
    }

    public static class GenerateClassResult {
        public Class<?> clz;
        public Schema schema;
        public Serializer<?> serializer;
        public Map<String, String> failedFieldInfo = new HashMap<>();
        public boolean success;

        public GenerateClassResult(Class<?> clz) {
            this.clz = clz;
        }

        public GenerateClassResult(Class<?> clz, Schema schema) {
            this.clz = clz;
            this.schema = schema;
            this.success = true;
        }
    }
}
