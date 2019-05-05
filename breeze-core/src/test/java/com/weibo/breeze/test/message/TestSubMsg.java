package com.weibo.breeze.test.message;

import com.weibo.breeze.BreezeBuffer;
import com.weibo.breeze.BreezeException;
import com.weibo.breeze.BreezeReader;
import com.weibo.breeze.BreezeWriter;
import com.weibo.breeze.message.Message;
import com.weibo.breeze.message.Schema;

import java.util.*;

/**
 * Created by zhanglei28 on 2019/3/27.
 */
public class TestSubMsg implements Message {

    private static final Schema schema = new Schema();

    static {
        try {
            schema.setName("TestSubMsg")
                    .putField(new Schema.Field(1, "string", "string"))
                    .putField(new Schema.Field(2, "anInt", "int32"))
                    .putField(new Schema.Field(3, "aLong", "int64"))
                    .putField(new Schema.Field(4, "aFloat", "float32"))
                    .putField(new Schema.Field(5, "aDouble", "float64"))
                    .putField(new Schema.Field(6, "aByte", "byte"))
                    .putField(new Schema.Field(7, "bytes", "byte[]"))
                    .putField(new Schema.Field(8, "map", "map<string, byte[]>"))
                    .putField(new Schema.Field(9, "map2", "map<int32, list<message>>"))
                    .putField(new Schema.Field(10, "list", "list<int32>"))
                    .putField(new Schema.Field(11, "aBoolean", "bool"));
        } catch (BreezeException ignore) {
        }
    }

    private String string;
    private int anInt;
    private long aLong;
    private float aFloat;
    private double aDouble;
    private byte aByte;
    private byte[] bytes;
    private Map<String, byte[]> map;
    private Map<Integer, List> map2;
    private List<Integer> list;
    private boolean aBoolean;

    @Override
    public void writeToBuf(BreezeBuffer buffer) throws BreezeException {
        BreezeWriter.writeMessage(buffer, schema.getName(), () -> {
            BreezeWriter.writeMessageField(buffer, 1, string);
            BreezeWriter.writeMessageField(buffer, 2, anInt);
            BreezeWriter.writeMessageField(buffer, 3, aLong);
            BreezeWriter.writeMessageField(buffer, 4, aFloat);
            BreezeWriter.writeMessageField(buffer, 5, aDouble);
            BreezeWriter.writeMessageField(buffer, 6, aByte);
            BreezeWriter.writeMessageField(buffer, 7, bytes);
            BreezeWriter.writeMessageField(buffer, 8, map);
            BreezeWriter.writeMessageField(buffer, 9, map2);
            BreezeWriter.writeMessageField(buffer, 10, list);
            BreezeWriter.writeMessageField(buffer, 11, aBoolean);
        });
    }

    @Override
    public Message readFromBuf(BreezeBuffer buffer) throws BreezeException {
        BreezeReader.readMessage(buffer, false, (int index) -> {
            switch (index) {
                case 1:
                    string = BreezeReader.readString(buffer);
                    break;
                case 2:
                    anInt = BreezeReader.readInt32(buffer);
                    break;
                case 3:
                    aLong = BreezeReader.readInt64(buffer);
                    break;
                case 4:
                    aFloat = BreezeReader.readFloat32(buffer);
                    break;
                case 5:
                    aDouble = BreezeReader.readFloat64(buffer);
                    break;
                case 6:
                    aByte = BreezeReader.readByte(buffer);
                    break;
                case 7:
                    bytes = BreezeReader.readBytes(buffer);
                    break;
                case 8:
                    map = new HashMap<>();
                    BreezeReader.readMap(buffer, map, String.class, byte[].class);
                    break;
                case 9:
                    map2 = new HashMap<>();
                    BreezeReader.readMap(buffer, map2, Integer.class, List.class);
                    break;
                case 10:
                    list = new ArrayList<>();
                    BreezeReader.readCollection(buffer, list, Integer.class);
                    break;
                case 11:
                    aBoolean = BreezeReader.readBool(buffer);
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

    public String getString() {
        return string;
    }

    public void setString(String string) {
        this.string = string;
    }

    public int getAnInt() {
        return anInt;
    }

    public void setAnInt(int anInt) {
        this.anInt = anInt;
    }

    public long getaLong() {
        return aLong;
    }

    public void setaLong(long aLong) {
        this.aLong = aLong;
    }

    public float getaFloat() {
        return aFloat;
    }

    public void setaFloat(float aFloat) {
        this.aFloat = aFloat;
    }

    public double getaDouble() {
        return aDouble;
    }

    public void setaDouble(double aDouble) {
        this.aDouble = aDouble;
    }

    public byte getaByte() {
        return aByte;
    }

    public void setaByte(byte aByte) {
        this.aByte = aByte;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public Map<String, byte[]> getMap() {
        return map;
    }

    public void setMap(Map<String, byte[]> map) {
        this.map = map;
    }

    public Map<Integer, List> getMap2() {
        return map2;
    }

    public void setMap2(Map<Integer, List> map2) {
        this.map2 = map2;
    }

    public List<Integer> getList() {
        return list;
    }

    public void setList(List<Integer> list) {
        this.list = list;
    }

    public boolean isaBoolean() {
        return aBoolean;
    }

    public void setaBoolean(boolean aBoolean) {
        this.aBoolean = aBoolean;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TestSubMsg that = (TestSubMsg) o;

        if (anInt != that.anInt) return false;
        if (aLong != that.aLong) return false;
        if (Float.compare(that.aFloat, aFloat) != 0) return false;
        if (Double.compare(that.aDouble, aDouble) != 0) return false;
        if (aByte != that.aByte) return false;
        if (aBoolean != that.aBoolean) return false;
        if (string != null ? !string.equals(that.string) : that.string != null) return false;
        if (!Arrays.equals(bytes, that.bytes)) return false;
        if (map2 != null ? !map2.equals(that.map2) : that.map2 != null) return false;
        return list != null ? list.equals(that.list) : that.list == null;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = string != null ? string.hashCode() : 0;
        result = 31 * result + anInt;
        result = 31 * result + (int) (aLong ^ (aLong >>> 32));
        result = 31 * result + (aFloat != +0.0f ? Float.floatToIntBits(aFloat) : 0);
        temp = Double.doubleToLongBits(aDouble);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (int) aByte;
        result = 31 * result + Arrays.hashCode(bytes);
        result = 31 * result + (map2 != null ? map2.hashCode() : 0);
        result = 31 * result + (list != null ? list.hashCode() : 0);
        result = 31 * result + (aBoolean ? 1 : 0);
        return result;
    }
}
