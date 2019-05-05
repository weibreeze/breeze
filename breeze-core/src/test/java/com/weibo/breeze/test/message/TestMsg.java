package com.weibo.breeze.test.message;

import com.weibo.breeze.BreezeBuffer;
import com.weibo.breeze.BreezeException;
import com.weibo.breeze.BreezeReader;
import com.weibo.breeze.BreezeWriter;
import com.weibo.breeze.message.Message;
import com.weibo.breeze.message.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhanglei28 on 2019/3/21.
 */
public class TestMsg implements Message {
    private int anInt;
    private String string;
    private Map<String, TestSubMsg> map;
    private List<TestSubMsg> list;
    private static final Schema schema = new Schema();

    static {
        try {
            schema.setName("TestMsg")
                    .putField(new Schema.Field(1, "anInt", "int32"))
                    .putField(new Schema.Field(2, "string", "string"))
                    .putField(new Schema.Field(3, "map", "map<string, message<TestSubMsg>>"))
                    .putField(new Schema.Field(4, "list", "list<message<TestSubMsg>>"));
        } catch (BreezeException ignore) {
        }
    }


    @Override
    public void writeToBuf(BreezeBuffer buffer) throws BreezeException {
        BreezeWriter.writeMessage(buffer, schema.getName(), () -> {
            BreezeWriter.writeMessageField(buffer, 1, anInt);
            BreezeWriter.writeMessageField(buffer, 2, string);
            BreezeWriter.writeMessageField(buffer, 3, map);
            BreezeWriter.writeMessageField(buffer, 4, list);
        });
    }

    @Override
    public TestMsg readFromBuf(BreezeBuffer buffer) throws BreezeException {
        BreezeReader.readMessage(buffer, false, (int index) -> {
            switch (index) {
                case 1:
                    anInt = BreezeReader.readInt32(buffer);
                    break;
                case 2:
                    string = BreezeReader.readString(buffer);
                    break;
                case 3:
                    map = new HashMap<>();
                    BreezeReader.readMap(buffer, map, String.class, TestSubMsg.class);
                    break;
                case 4:
                    list = new ArrayList<>();
                    BreezeReader.readCollection(buffer, list, TestSubMsg.class);
                    break;
                default: // skip unknown field
                    BreezeReader.readObject(buffer, Object.class);
            }
        });
        return this;
    }

    @Override
    public String getName() {
        return schema.getName();
    }

    @Override
    public String getAlias() {
        return schema.getAlias();
    }

    @Override
    public Message getDefaultInstance() {
        return new TestMsg();
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    public int getAnInt() {
        return anInt;
    }

    public void setAnInt(int anInt) {
        this.anInt = anInt;
    }

    public String getString() {
        return string;
    }

    public void setString(String string) {
        this.string = string;
    }

    public Map<String, TestSubMsg> getMap() {
        return map;
    }

    public void setMap(Map<String, TestSubMsg> map) {
        this.map = map;
    }

    public List<TestSubMsg> getList() {
        return list;
    }

    public void setList(List<TestSubMsg> list) {
        this.list = list;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TestMsg testMsg = (TestMsg) o;

        if (anInt != testMsg.anInt) return false;
        if (string != null ? !string.equals(testMsg.string) : testMsg.string != null) return false;
        if (map != null ? !map.equals(testMsg.map) : testMsg.map != null) return false;
        return list != null ? list.equals(testMsg.list) : testMsg.list == null;
    }

    @Override
    public int hashCode() {
        int result = anInt;
        result = 31 * result + (string != null ? string.hashCode() : 0);
        result = 31 * result + (map != null ? map.hashCode() : 0);
        result = 31 * result + (list != null ? list.hashCode() : 0);
        return result;
    }
}
