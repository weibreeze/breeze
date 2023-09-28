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

package com.weibo.breeze.serializer;

import com.weibo.breeze.*;
import com.weibo.breeze.message.Schema;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

/**
 * @author zhanglei28
 * @date 2019/3/27.
 */
@SuppressWarnings("unchecked")
public class CommonSerializer<T> implements Serializer<T> {
    public static boolean WITH_STATIC_FIELD = false;
    private String[] names;
    private Class<T> clz;
    private Method buildMethod;
    private Object buildObject;
    private Schema schema;
    private String cleanName;
    private Map<Integer, Integer> fieldHashIndexMap = new HashMap<>(0); // Store the hash value of the field name. Used to solve deserialize compatibility


    public CommonSerializer(Class<T> clz) throws BreezeException {
        checkClass(clz);
        Map<String, Field> target = new HashMap<>();
        // public fields
        Field[] fields = clz.getFields();
        for (Field field : fields) {
            if (Modifier.isFinal(field.getModifiers())) {
                continue;
            }
            if (!WITH_STATIC_FIELD && Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            target.put(field.getName(), field);
        }
        // fields with getter method.
        Method[] methods = clz.getMethods();
        if (methods.length > 0) {
            List<Field> list = getAllFields(clz);
            for (Field field : list) {
                if (!target.containsKey(field.getName())
                        && !Modifier.isFinal(field.getModifiers())
                        && (WITH_STATIC_FIELD || !Modifier.isStatic(field.getModifiers()))) {
                    // if it has getter method
                    for (Method method : methods) {
                        if (method.getName().equalsIgnoreCase("get" + field.getName())
                                || ((field.getType() == boolean.class)
                                && method.getName().equalsIgnoreCase("is" + field.getName()))) {
                            target.put(field.getName(), field);
                            break;
                        }
                    }
                }
            }
        }
        if (target.isEmpty()) {
            throw new BreezeException("field is empty");
        }
        schema = new Schema();
        schema.setName(clz.getName()).setPrimitive(false);
        for (Map.Entry<String, Field> entry : target.entrySet()) {
            schema.putField(new Schema.Field(getHash(entry.getKey()), entry.getKey(), entry.getValue().getType().getName(), entry.getValue()));
        }

        this.clz = clz;
        cleanName = Breeze.getCleanName(clz.getName());
        if (clz.getName().contains("$")) {
            names = new String[]{cleanName, clz.getName()};
        } else {
            names = new String[]{cleanName};
        }
    }


    public CommonSerializer(Schema schema) throws BreezeException {
        if (schema == null) {
            throw new BreezeException("BreezeSchema must not null when create CommonSerializer");
        }
        try {
            clz = (Class<T>) Class.forName(schema.getJavaName() != null ? schema.getJavaName() : schema.getName(), true, Schema.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new BreezeException("can not create class in CommonSerializer, class name:" + schema.getName());
        }
        checkClass(clz);
        Map<Integer, Schema.Field> fieldMap = schema.getFields();
        if (fieldMap.isEmpty()) {
            throw new BreezeException("schema field is empty");
        }
        for (Map.Entry<Integer, Schema.Field> entry : fieldMap.entrySet()) {
            try {
                entry.getValue().setField(getField(clz, entry.getValue().getName()));
                fieldHashIndexMap.put(getHash(entry.getValue().getName()), entry.getKey());
            } catch (NoSuchFieldException e) {
                throw new BreezeException("can not get field from class " + clz.getName() + ", field:" + entry.getValue().getName());
            }
        }
        this.schema = schema;
        cleanName = Breeze.getCleanName(clz.getName());
        if (clz.getName().contains("$")) {
            names = new String[]{cleanName, clz.getName()};
        } else {
            names = new String[]{cleanName};
        }
    }

    public static int getHash(String name) {
        return name.hashCode() & 0x7fffffff;
    }

    private static List<Field> getAllFields(Class<?> clz) {
        ArrayList<Field> list = new ArrayList<>();
        Field[] fields;
        do {
            fields = clz.getDeclaredFields();
            Collections.addAll(list, fields);
            clz = clz.getSuperclass();
        } while (clz != null && clz != Object.class);
        return list;
    }

    private static Field getField(Class<?> clz, String name) throws NoSuchFieldException {
        Field field;
        do {
            try {
                field = clz.getDeclaredField(name);
                return field;
            } catch (NoSuchFieldException ignore) {
            }
            clz = clz.getSuperclass();
        } while (clz != null && clz != Object.class);
        throw new NoSuchFieldException();
    }

    @Override
    public void writeToBuf(Object obj, BreezeBuffer buffer) throws BreezeException {
        BreezeWriter.writeMessage(buffer, () -> {
            Map<Integer, Schema.Field> fieldMap = schema.getFields();
            for (Map.Entry<Integer, Schema.Field> entry : fieldMap.entrySet()) {
                entry.getValue().writeField(buffer, obj);
            }
        });
    }

    @Override
    public T readFromBuf(BreezeBuffer buffer) throws BreezeException {
        final T t = newInstance();
        BreezeReader.readMessage(buffer, (int index) -> {
            Schema.Field field = schema.getFieldByIndex(index);
            if (field == null && schema.isPrimitive() && fieldHashIndexMap.containsKey(index)) {
                field = schema.getFieldByIndex(fieldHashIndexMap.get(index)); // try using hash index
            }
            if (field == null) { // ignore unknown fields
                BreezeReader.readObject(buffer, Object.class);
                return;
            }
            field.readField(buffer, t);
        });
        return t;
    }

    @Override
    public String[] getNames() {
        return names;
    }

    public T newInstance() throws BreezeException {
        try {
            if (buildMethod != null) {
                return (T) buildMethod.invoke(buildObject);
            }
            return clz.newInstance();
        } catch (ReflectiveOperationException e1) {
            throw new BreezeException("CommonSerializer read fail. can not create default object. class:" + clz);
        }
    }

    private void checkClass(Class<T> clz) throws BreezeException {
        if (clz == null) {
            throw new BreezeException("class must not null when create CommonSerializer");
        }
        try {
            clz.newInstance();
        } catch (ReflectiveOperationException e) {
            // if it has builder like lombok
            try {
                Method method = clz.getMethod("builder");
                if (Modifier.isStatic(method.getModifiers())) {
                    Object object = method.invoke(null);
                    if (object != null) {
                        method = object.getClass().getMethod("build");
                        Object obj = method.invoke(object);
                        if (clz.isInstance(obj)) {
                            buildMethod = method;
                            buildObject = object;
                            return;
                        }
                    }
                }
            } catch (ReflectiveOperationException ignore) {
            }
            throw new BreezeException("class must has constructor without arguments, or has builder like lombok.");
        }
    }

}
