package com.weibo.breeze.message;

import com.weibo.breeze.BreezeBuffer;
import com.weibo.breeze.BreezeException;
import com.weibo.breeze.BreezeReader;
import com.weibo.breeze.BreezeWriter;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhanglei28 on 2019/3/21.
 */
public class GenericMessage implements Message {
    private Map<Integer, Object> fields = new HashMap<>();// field 0 is reserved for schema
    private Schema schema;
    private String name = "GenericMessage";
    private String alias;//alias for class name.

    public int getSize() {
        return fields.size();
    }

    public Object getFieldByIndex(int index) {
        return fields.get(index);
    }

    public Object getFieldByName(String name) throws BreezeException {
        if (schema == null) {
            throw new BreezeException("without schema");
        }
        Schema.Field field = schema.getFieldByName(name);
        if (field != null) {
            return fields.get(field.getIndex());
        }
        return null;
    }

    @Override
    public void writeToBuf(BreezeBuffer buffer) throws BreezeException {
        BreezeWriter.writeMessage(buffer, getName(), getFields());
    }

    @Override
    public GenericMessage readFromBuf(BreezeBuffer buffer) throws BreezeException {
        BreezeReader.readFields(buffer, fields);
        //TODO process schema if has field 0
        return this;
    }

    public Map<Integer, Object> getFields() {
        return fields;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getAlias() {
        return alias;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public Message getDefaultInstance() {
        return new GenericMessage();
    }

    public void putFields(Integer index, Object field) {
        fields.put(index, field);
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }
}
