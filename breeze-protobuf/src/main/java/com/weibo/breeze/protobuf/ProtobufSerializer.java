package com.weibo.breeze.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Message;
import com.weibo.breeze.BreezeBuffer;
import com.weibo.breeze.BreezeException;
import com.weibo.breeze.BreezeReader;
import com.weibo.breeze.BreezeWriter;
import com.weibo.breeze.serializer.Serializer;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhanglei28 on 2019/4/1.
 */
@SuppressWarnings("all")
public class ProtobufSerializer<T extends Message> implements Serializer<T> {
    private Message defaultInstance;
    String[] names;
    Map<Integer, Descriptors.FieldDescriptor> fields;

    public ProtobufSerializer(Class<Message> clz) throws BreezeException {
        try {
            Method method = clz.getDeclaredMethod("getDefaultInstance");
            defaultInstance = (Message) method.invoke(null);
            names = new String[]{clz.getName()};
            fields = new HashMap<>();
            for (Descriptors.FieldDescriptor fieldDescriptor : defaultInstance.newBuilderForType().getDescriptorForType().getFields()) {
                fields.put(fieldDescriptor.getIndex(), fieldDescriptor);
            }
        } catch (Exception e) {
            throw new BreezeException("can not get a defaultInstance fo protobuf Message. e:" + e.getMessage());
        }
    }

    @Override
    public void writeToBuf(Message obj, BreezeBuffer buffer) throws BreezeException {
        BreezeWriter.writeMessage(buffer, obj.getClass().getName(), () -> {
            for (Descriptors.FieldDescriptor field : obj.getDescriptorForType().getFields()) {
                if (field.isOptional()
                        && field.getJavaType() == JavaType.MESSAGE
                        && !obj.hasField(field)) {
                    continue;
                }
                Object fieldValue = obj.getField(field);
                if (field.isMapField()) {
                    Descriptors.Descriptor type = field.getMessageType();
                    Descriptors.FieldDescriptor keyField = type.findFieldByName("key");
                    Descriptors.FieldDescriptor valueField = type.findFieldByName("value");
                    if (keyField == null || valueField == null) {
                        throw new BreezeException("Invalid protobuf map field.");
                    }
                    Map map = new HashMap();
                    for (Object element : (List) fieldValue) {
                        Message entry = (Message) element;
                        map.put(entry.getField(keyField), entry.getField(valueField));
                    }
                    if (!map.isEmpty()) {
                        BreezeWriter.writeMessageField(buffer, field.getIndex(), map);
                    }
                    continue;
                }
                if (field.getJavaType() == JavaType.ENUM) {
                    BreezeWriter.writeMessageField(buffer, field.getIndex(), ((Descriptors.EnumValueDescriptor) fieldValue).getNumber());
                    continue;
                }
                BreezeWriter.writeMessageField(buffer, field.getIndex(), fieldValue);
            }
        });
    }

    @Override
    public T readFromBuf(BreezeBuffer buffer) throws BreezeException {
        Message.Builder builder = defaultInstance.newBuilderForType();
        BreezeException[] exception = new BreezeException[]{null};
        BreezeReader.readMessage(buffer, (int index) -> {
            Descriptors.FieldDescriptor field = fields.get(index);
            if (field == null) {
                exception[0] = new BreezeException(names[0] + " not have field. index:" + index);
                return;
            }
            if (field.isMapField()) {
                Map<?, ?> map = BreezeReader.readObject(buffer, HashMap.class);
                if (map != null && !map.isEmpty()) {
                    Descriptors.Descriptor type = field.getMessageType();
                    Descriptors.FieldDescriptor keyField = type.findFieldByName("key");
                    Descriptors.FieldDescriptor valueField = type.findFieldByName("value");
                    if (keyField == null || valueField == null) {
                        throw new BreezeException("Invalid ptotobuf map field: " + field.getFullName());
                    }
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        Message.Builder entryBuilder = builder.newBuilderForField(field);
                        entryBuilder.setField(keyField, entry.getKey());
                        entryBuilder.setField(valueField, entry.getValue());
                        builder.addRepeatedField(field, entryBuilder.build());
                    }
                }
                return;
            }
            // must after map check.
            if (field.isRepeated()) {
                builder.setField(field, BreezeReader.readObject(buffer, ArrayList.class));
                return;
            }
            if (field.getJavaType() == JavaType.ENUM) {
                int intValue = BreezeReader.readObject(buffer, Integer.class);
                Descriptors.EnumValueDescriptor result;
                if (field.getEnumType().getFile().getSyntax() == Descriptors.FileDescriptor.Syntax.PROTO3) {
                    result = field.getEnumType().findValueByNumberCreatingIfUnknown(intValue);
                } else {
                    result = field.getEnumType().findValueByNumber(intValue);
                }
                builder.setField(field, result);
                return;
            }
            try {
                Class clz = defaultInstance.getField(field).getClass();
                builder.setField(field, BreezeReader.readObject(buffer, clz));
            } catch (BreezeException e) {
                exception[0] = e;
            }
        });
        if (exception[0] != null) {
            throw exception[0];
        }
        return (T) builder.build();
    }

    @Override
    public String[] getNames() {
        return names;
    }
}
