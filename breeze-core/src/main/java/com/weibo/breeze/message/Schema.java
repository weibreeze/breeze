package com.weibo.breeze.message;

import com.weibo.breeze.BreezeException;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhanglei28 on 2019/3/21.
 * <p>
 * not
 */
public class Schema {
    private static final Map<String, String> typeMap = new HashMap<>();

    static {
        typeMap.put("string", "java.lang.String");
        typeMap.put("map", "java.util.Map");
        typeMap.put("list", "java.util.List");
        typeMap.put("byte[]", "[B");
        typeMap.put("bool", "boolean");
        typeMap.put("int16", "short");
        typeMap.put("int32", "int");
        typeMap.put("in64", "long");
        typeMap.put("float32", "float");
        typeMap.put("float64", "double");
    }

    private String name;//class name
    private String alias;
    private boolean primitive = true; // is generated from schema. true : from schema; false : from class dynamically
    private Map<String, Field> fieldNameMap = new HashMap<>();
    private Map<Integer, Field> fieldIndexMap = new HashMap<>(0);

    public static Schema newSchema(String name) {
        return new Schema().setName(name);
    }

    public Schema putField(int index, String name) throws BreezeException {
        return putField(index, name, null);
    }

    public Schema putField(int index, String name, String type) throws BreezeException {
        return putField(new Field(index, name, type));
    }

    public Schema putField(Field field) throws BreezeException {
        if (field != null) {
            Field old = fieldNameMap.put(field.getName(), field);
            if (old != null) {
                throw new BreezeException("field name is same. schema: " + name + " field name:" + field.getName());
            }
            old = fieldIndexMap.put(field.getIndex(), field);
            if (old != null) {
                throw new BreezeException("field index is same. schema: " + name + ", index: " + field.getIndex() + ", field name:" + field.getName());
            }
        }
        return this;
    }

    public Field getFieldByName(String name) {
        return fieldNameMap.get(name);
    }

    public Field getFieldByIndex(int index) {
        return fieldIndexMap.get(index);
    }

    public Map<Integer, Field> getFields() {
        return fieldIndexMap;
    }

    public String getName() {
        return this.name;
    }

    public String getAlias() {
        return alias;
    }

    public boolean isPrimitive() {
        return primitive;
    }

    public Schema setName(String name) {
        this.name = name;
        return this;
    }

    public Schema setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    public Schema setPrimitive(boolean primitive) {
        this.primitive = primitive;
        return this;
    }

    public static class Field {
        private int index;
        private String name;
        private String type;
        private java.lang.reflect.Field field;

        public Field(int index, String name, String type) throws BreezeException {
            this(index, name, type, null);
        }

        public Field(int index, String name, String type, java.lang.reflect.Field field) throws BreezeException {
            if (index < 1) {// -1:not exist; 0:schema
                throw new BreezeException("schema field index should great than 0");
            }
            if (field != null) {
                field.setAccessible(true);
            }
            if (name == null) {
                throw new BreezeException("schema field name must not null");
            }
            this.index = index;
            this.name = name.trim();
            this.type = type;
            this.field = field;
        }

        public String getDesc() {
            return "Field{" +
                    "index=" + index +
                    ", name='" + name + '\'' +
                    ", type='" + type + '\'' +
                    '}';
        }

        public int getIndex() {
            return index;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public void setField(java.lang.reflect.Field field) {
            if (field != null) {
                field.setAccessible(true);
            }
            this.field = field;
        }

        public Object getFieldInstance(Object object) throws IllegalAccessException {
            return field.get(object);
        }

        public Class<?> getFieldClass() {
            return field.getType();
        }

        public Type getGenericType() {
            return field.getGenericType();
        }

        public void fill(Object target, Object fieldObject) throws IllegalAccessException {
            field.set(target, fieldObject);
        }
    }

}
