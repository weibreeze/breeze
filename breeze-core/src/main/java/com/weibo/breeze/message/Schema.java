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

package com.weibo.breeze.message;

import com.weibo.breeze.*;
import com.weibo.breeze.type.BreezeType;
import com.weibo.breeze.type.Types;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhanglei28
 * @date 2019/3/21.
 */
public class Schema {
    private String name;//schema name
    private String alias;
    private String javaName; // java class name. it will null if not a inner class.
    private boolean primitive = true; // is generated from schema. true : from schema; false : from class dynamically
    private Map<String, Field> fieldNameMap = new HashMap<>();
    private Map<Integer, Field> fieldIndexMap = new HashMap<>(0);
    private boolean isEnum;
    private Map<Integer, String> enumValues = new HashMap<>(0);
    private Map<String, Integer> enumNameMap = new HashMap<>(0);

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

    public Schema setName(String name) {
        this.name = name;
        return this;
    }

    public String getAlias() {
        return alias;
    }

    public Schema setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    public boolean isPrimitive() {
        return primitive;
    }

    public Schema setPrimitive(boolean primitive) {
        this.primitive = primitive;
        return this;
    }

    public String getJavaName() {
        return javaName;
    }

    public void setJavaName(String javaName) {
        this.javaName = javaName;
    }

    public boolean isEnum() {
        return isEnum;
    }

    public void setEnum(boolean anEnum) {
        isEnum = anEnum;
    }

    public Map<Integer, String> getEnumValues() {
        return enumValues;
    }

    public Integer getEnumNumber(String name) {
        return enumNameMap.get(name);
    }

    public Schema addEnumValue(Integer number, String value) {
        enumValues.put(number, value);
        enumNameMap.put(value, number);
        return this;
    }

    public static class Field {
        private int index;
        private String name;
        private String type;
        private BreezeType breezeType;
        private java.lang.reflect.Field field;
        private boolean checked;// check breeze type by lazy

        public Field(int index, String name, String type) throws BreezeException {
            this(index, name, type, null);
        }

        public Field(int index, String name, String type, java.lang.reflect.Field field) throws BreezeException {
            if (index < 1) {// -1:not exist; 0:schema
                throw new BreezeException("schema field index should great than 0");
            }
            if (field != null) {
                field.setAccessible(true);
                breezeType = Breeze.getBreezeType(field.getGenericType());
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

        public BreezeType getBreezeType() {
            return breezeType;
        }

        public void setBreezeType(BreezeType breezeType) {
            this.breezeType = breezeType;
        }

        public void setField(java.lang.reflect.Field field) throws BreezeException {
            if (field != null) {
                field.setAccessible(true);
                Type type = field.getGenericType();
                breezeType = Breeze.getBreezeType(type);
                if (breezeType.getType() == Types.PACKED_ARRAY) {
                    if (type instanceof ParameterizedType) {
                        type = ((ParameterizedType) type).getRawType();
                    }
                    if (type instanceof Class && !((Class) type).isAssignableFrom(List.class)) {
                        breezeType = null; // 非list类型不使用breezeType进行编解码，使用Object兼容性会更好
                        checked = true;
                    }
                }
            }
            this.field = field;
        }

        public Object getFieldInstance(Object object) throws IllegalAccessException {
            return field.get(object);
        }

        public void writeField(BreezeBuffer buffer, Object object) throws BreezeException {
            try {
                if (breezeType == null && !checked) {// lazy init breeze type if field class be circular referenced
                    breezeType = Breeze.getBreezeType(field.getGenericType());
                    checked = true;
                }
                if (breezeType != null) {
                    breezeType.writeMessageField(buffer, index, field.get(object));
                } else if (object != null) {
                    buffer.putVarint(index);
                    BreezeWriter.writeObject(buffer, field.get(object));
                }
            } catch (IllegalAccessException e) {
                throw new BreezeException("can not get field. class:" + field.getDeclaringClass() + ", field: " + name + ". e:" + e.getMessage());
            }
        }

        public void readField(BreezeBuffer buffer, Object object) throws BreezeException {
            try {
                Object fieldObject;
                if (breezeType == null && !checked) {// lazy init breeze type if field class be circular referenced
                    breezeType = Breeze.getBreezeType(field.getGenericType());
                    checked = true;
                }
                if (breezeType != null) {
                    fieldObject = breezeType.read(buffer);
                } else {
                    fieldObject = BreezeReader.readObjectByType(buffer, field.getGenericType());
                }
                field.set(object, fieldObject);
            } catch (IllegalAccessException e) {
                throw new BreezeException("can not set field. class:" + field.getDeclaringClass() + ", field: " + name + ". e:" + e.getMessage());
            }
        }

        public Class<?> getFieldClass() {
            return field.getType();
        }

        public Type getGenericType() {
            return field.getGenericType();
        }

    }

}
