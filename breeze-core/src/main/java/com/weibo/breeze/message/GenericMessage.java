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

import com.weibo.breeze.BreezeBuffer;
import com.weibo.breeze.BreezeException;
import com.weibo.breeze.BreezeReader;
import com.weibo.breeze.BreezeWriter;

import java.util.HashMap;
import java.util.Map;

import static com.weibo.breeze.BreezeReader.readObject;
import static com.weibo.breeze.BreezeWriter.writeObject;

/**
 * @author zhanglei28
 * @date 2019/3/21.
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
        BreezeWriter.writeMessage(buffer, () -> {
            for (Map.Entry<Integer, Object> entry : fields.entrySet()) {
                if (entry.getValue() != null) {
                    buffer.putVarint(entry.getKey());
                    writeObject(buffer, entry.getValue());
                }
            }
        });
    }

    @Override
    public GenericMessage readFromBuf(BreezeBuffer buffer) throws BreezeException {
        BreezeReader.readMessage(buffer, (int index) -> fields.put(index, readObject(buffer, Object.class)));
        return this;
    }

    public Map<Integer, Object> getFields() {
        return fields;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    @Override
    public Message getDefaultInstance() {
        return new GenericMessage();
    }

    public void putFields(Integer index, Object field) {
        fields.put(index, field);
    }
}
