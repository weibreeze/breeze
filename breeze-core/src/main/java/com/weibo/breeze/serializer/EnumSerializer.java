package com.weibo.breeze.serializer;

import com.weibo.breeze.BreezeBuffer;
import com.weibo.breeze.BreezeException;
import com.weibo.breeze.BreezeReader;
import com.weibo.breeze.BreezeWriter;

import java.lang.reflect.Method;

/**
 * Created by zhanglei28 on 2019/3/29.
 */
public class EnumSerializer implements Serializer<Enum> {
    private Class enumClz;
    private Method valueOf;

    public EnumSerializer(Class enumClz) throws BreezeException {
        if (!enumClz.isEnum()) {
            throw new BreezeException("class type must be enum in EnumSerializer. real type :" + enumClz.getName());
        }
        this.enumClz = enumClz;
        try {
            this.valueOf = enumClz.getMethod("valueOf", new Class[]{Class.class, String.class});
        } catch (NoSuchMethodException e) {
            throw new BreezeException("create EnumSerializer fail. e:" + e.getMessage());
        }
    }

    @Override
    public void writeToBuf(Enum obj, BreezeBuffer buffer) throws BreezeException {
        BreezeWriter.writeMessage(buffer, obj.getClass().getName(), () -> {
            BreezeWriter.writeMessageField(buffer, 1, obj.name());
        });
    }

    @Override
    public Enum readFromBuf(BreezeBuffer buffer) throws BreezeException {
        Object[] objects = new Object[2];
        BreezeReader.readMessage(buffer, true, (int index) -> {
            switch (index) {
                case 1:
                    try {
                        objects[0] = valueOf.invoke(null, enumClz, BreezeReader.readString(buffer));
                    } catch (ReflectiveOperationException e) {
                        objects[1] = e;
                    }
                    break;
            }
        });
        if (objects[0] != null) {
            return (Enum) objects[0];
        }
        throw new BreezeException("read from buf fail in EnumSerializer. e:" + ((Exception) objects[1]).getMessage());
    }

    @Override
    public String[] getNames() {
        return new String[]{enumClz.getName()};
    }
}
