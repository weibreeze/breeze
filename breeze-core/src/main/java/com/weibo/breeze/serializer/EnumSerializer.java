package com.weibo.breeze.serializer;

import com.weibo.breeze.*;
import com.weibo.breeze.message.Schema;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhanglei28 on 2019/3/29.
 */
public class EnumSerializer implements Serializer<Enum> {
    private Class enumClz;
    private Method valueOf;
    private Schema schema;
    private Map<Integer, Field> enumFields;
    private String cleanName;

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
            enumFields = new HashMap<>();
            for (com.weibo.breeze.message.Schema.Field sfield : schema.getFields().values()) {
                if ("enumValue".equals(sfield.getName())) {
                    continue;
                }
                try {
                    Field field = enumClz.getDeclaredField(sfield.getName());
                    field.setAccessible(true);
                    enumFields.put(sfield.getIndex(), field);
                } catch (NoSuchFieldException e) {
                    throw new BreezeException("create EnumSerializer fail. e:" + e.getMessage());
                }
            }
        }
        cleanName = Breeze.getCleanName(enumClz.getName());
    }

    @Override
    public void writeToBuf(Enum obj, BreezeBuffer buffer) throws BreezeException {
        if (schema != null && schema.isEnum()) { // only write enum number when has enum schema
            Integer number = schema.getEnumNumber(obj.name());
            if (number == null) {
                throw new BreezeException("unknown enum cleanName in breeze schema. class:" + enumClz.getName() + ", enum cleanName:" + obj.name());
            }
            BreezeWriter.writeMessage(buffer, cleanName, () -> BreezeWriter.writeMessageField(buffer, 1, number));
        } else {
            // write enum cleanName when not formal enum
            BreezeWriter.writeMessage(buffer, cleanName, () -> {
                BreezeWriter.writeMessageField(buffer, 1, obj.name());
                // write additional fields according schema
                if (enumFields != null) {
                    for (Map.Entry<Integer, Field> entry : enumFields.entrySet()) {
                        try {
                            BreezeWriter.writeMessageField(buffer, entry.getKey(), entry.getValue().get(obj));
                        } catch (IllegalAccessException e) {
                            throw new BreezeException("can not get enum field. class:" + obj.getClass() + ", field: " + entry.getValue().getName() + ". e:" + e.getMessage());
                        }
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
                        int number = BreezeReader.readInt32(buffer, true);
                        name = schema.getEnumValues().get(number);
                        if (name == null) {
                            objects[1] = new BreezeException("unknown enum number " + number);
                            return;
                        }
                    } else {
                        name = BreezeReader.readString(buffer, true);
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
        if (enumClz.getName().contains("$")) {
            return new String[]{cleanName, enumClz.getName()};
        }
        return new String[]{cleanName};
    }
}
