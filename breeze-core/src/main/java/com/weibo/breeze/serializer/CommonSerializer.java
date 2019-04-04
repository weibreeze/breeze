package com.weibo.breeze.serializer;

import com.weibo.breeze.BreezeBuffer;
import com.weibo.breeze.BreezeException;
import com.weibo.breeze.BreezeReader;
import com.weibo.breeze.BreezeWriter;
import com.weibo.breeze.message.Schema;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhanglei28 on 2019/3/27.
 */
public class CommonSerializer<T> implements Serializer<T> {
    public static boolean WITH_STATIC_FIELD = false;
    Class<T> clz;
    Method buildMethod;
    Object buildObject;
    Schema schema;
    ArrayList<String> names = new ArrayList<>();

    public CommonSerializer(Class<T> clz) throws BreezeException {
        checkClass(clz);
        Map<String, Field> target = new HashMap<>();
        // public fields
        Field[] fields = clz.getFields();
        for (Field field : fields) {
            if (Modifier.isFinal(field.getModifiers())){
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
                    // if has getter method
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
        names.add(clz.getName());
    }

    public CommonSerializer(Schema schema) throws BreezeException {
        if (schema == null) {
            throw new BreezeException("BreezeSchema must not null when create CommonSerializer");
        }
        try {
            clz = (Class<T>) Class.forName(schema.getName(), true, Schema.class.getClassLoader());
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
            } catch (NoSuchFieldException e) {
                throw new BreezeException("can not get field from class " + clz.getName() + ", field:" + entry.getValue().getName());
            }
        }
        this.schema = schema;
        names.add(clz.getName());
    }

    @Override
    public void writeToBuf(Object obj, BreezeBuffer buffer) throws BreezeException {
        BreezeWriter.writeMessage(buffer, obj.getClass().getName(), () -> {
            Map<Integer, Schema.Field> fieldMap = schema.getFields();
            for (Map.Entry<Integer, Schema.Field> entry : fieldMap.entrySet()) {
                try {
                    BreezeWriter.writeMessageField(buffer, entry.getKey(), entry.getValue().getFieldInstance(obj));
                } catch (IllegalAccessException e) {
                    throw new BreezeException("CommonSerializer write fail. e:" + e.getMessage());
                }
            }
        });
    }

    @Override
    public T readFromBuf(BreezeBuffer buffer) throws BreezeException {
        T t = null;
        try {
            t = clz.newInstance();
        } catch (ReflectiveOperationException e) {
            if (buildMethod != null) {
                try {
                    t = (T) buildMethod.invoke(buildObject);
                } catch (ReflectiveOperationException e1) {
                }
            }
        }
        if (t == null) {
            throw new BreezeException("CommonSerializer read fail. can not create default object. class:" + clz);
        }
        final T ft = t;
        BreezeReader.readMessage(buffer, true, (int index) -> {
            Schema.Field field = schema.getFieldByIndex(index);
            if (field == null) {
                throw new BreezeException("not found field. class:" + clz.getName() + "index:" + index);
            }
            try {
                field.fill(ft, BreezeReader.readObject(buffer, field.getFieldClass()));
            } catch (IllegalAccessException e) {
                throw new BreezeException("CommonSerializer set field fail. e:" + e.getMessage());
            }
        });
        return ft;
    }

    @Override
    public String[] getNames() {
        String[] ns = new String[names.size()];
        names.toArray(ns);
        return ns;
    }

    public void addAlias(String alias) {
        names.add(alias);
    }

    public static int getHash(String name) {
        return name.hashCode() & 0x7fffffff;
    }

    private static List<Field> getAllFields(Class clz) {
        ArrayList<Field> list = new ArrayList<>();
        Field[] fields;
        do {
            fields = clz.getDeclaredFields();
            for (Field field : fields) {
                list.add(field);
            }
            clz = clz.getSuperclass();
        } while (clz != null && clz != Object.class);
        return list;
    }

    //get field not only public
    private static Field getField(Class clz, String name) throws NoSuchFieldException {
        Field field = null;
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

    private void checkClass(Class clz) throws BreezeException {
        if (clz == null) {
            throw new BreezeException("class must not null when create CommonSerializer");
        }
        try {
            clz.newInstance();
        } catch (ReflectiveOperationException e) {
            // if has builder like lombok
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
            } catch (ReflectiveOperationException e1) {
            }
            throw new BreezeException("class must has constructor with no arguments, or has builder like lombok.");
        }
    }

}
