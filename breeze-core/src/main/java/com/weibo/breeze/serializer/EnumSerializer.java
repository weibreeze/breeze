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

import java.lang.reflect.Method;

import static com.weibo.breeze.type.Types.TYPE_INT32;
import static com.weibo.breeze.type.Types.TYPE_STRING;

/**
 * @author zhanglei28
 * @date 2019/3/29.
 */
public class EnumSerializer implements Serializer<Enum> {
    private Class enumClz;
    private Method valueOf;
    private Schema schema;
    private String cleanName;
    private String[] names;

    @SuppressWarnings("unchecked")
    public EnumSerializer(Class enumClz) throws BreezeException {
        if (!enumClz.isEnum()) {
            throw new BreezeException("class type must be enum in EnumSerializer. real type :" + enumClz.getName());
        }
        this.enumClz = enumClz;
        try {
            this.valueOf = enumClz.getMethod("valueOf", Class.class, String.class);
        } catch (NoSuchMethodException e) {
            throw new BreezeException("create EnumSerializer fail. e:" + e.getMessage());
        }
        schema = SchemaLoader.loadSchema(enumClz.getName());
        if (schema != null && !schema.isEnum()) { // as message. add additional fields
            for (Schema.Field field : schema.getFields().values()) {
                if ("enumValue".equals(field.getName())) {
                    continue;
                }
                try {
                    field.setField(enumClz.getDeclaredField(field.getName()));
                } catch (NoSuchFieldException e) {
                    throw new BreezeException("create EnumSerializer fail. e:" + e.getMessage());
                }
            }
        }
        cleanName = Breeze.getCleanName(enumClz.getName());
        if (enumClz.getName().contains("$")) {
            names = new String[]{cleanName, enumClz.getName()};
        } else {
            names = new String[]{cleanName};
        }
    }

    @Override
    public void writeToBuf(Enum obj, BreezeBuffer buffer) throws BreezeException {
        if (schema != null && schema.isEnum()) { // only write enum number when has enum schema
            Integer number = schema.getEnumNumber(obj.name());
            if (number == null) {
                throw new BreezeException("unknown enum cleanName in breeze schema. class:" + enumClz.getName() + ", enum cleanName:" + obj.name());
            }
            BreezeWriter.writeMessage(buffer, () -> TYPE_INT32.writeMessageField(buffer, 1, number));
        } else {
            // write enum cleanName when not formal enum
            BreezeWriter.writeMessage(buffer, () -> {
                TYPE_STRING.writeMessageField(buffer, 1, obj.name());
                // write additional fields according schema
                if (schema != null && !schema.isEnum()) { // as message. add additional fields
                    for (Schema.Field field : schema.getFields().values()) {
                        if ("enumValue".equals(field.getName())) {
                            continue;
                        }
                        field.writeField(buffer, obj);
                    }
                }
            });
        }
    }

    @Override
    public Enum readFromBuf(BreezeBuffer buffer) throws BreezeException {
        Object[] objects = new Object[2];
        BreezeReader.readMessage(buffer, (int index) -> {
            switch (index) {
                case 1:
                    String name;
                    if (schema != null && schema.isEnum()) {
                        int number = BreezeReader.readInt32(buffer);
                        name = schema.getEnumValues().get(number);
                        if (name == null) {
                            objects[1] = new BreezeException("unknown enum number " + number);
                            return;
                        }
                    } else {
                        name = BreezeReader.readString(buffer);
                    }
                    try {
                        objects[0] = valueOf.invoke(null, enumClz, name);
                    } catch (ReflectiveOperationException e) {
                        objects[1] = e;
                    }
                    break;
                default:// ignore
                    BreezeReader.readObject(buffer, Object.class);
            }
        });
        if (objects[0] != null) {
            return (Enum) objects[0];
        }
        throw new BreezeException("read from buf fail in EnumSerializer. class:" + enumClz.getName() + ", e:" + ((Exception) objects[1]).getMessage());
    }

    @Override
    public String[] getNames() {
        return names;
    }
}
