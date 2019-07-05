package com.weibo.breeze.test.serializer;

import com.weibo.breeze.*;
import com.weibo.breeze.serializer.Serializer;
import com.weibo.breeze.test.obj.TestSubObj;
import com.weibo.breeze.type.BreezeType;

import java.util.Map;

import static com.weibo.breeze.type.Types.TYPE_INT32;
import static com.weibo.breeze.type.Types.TYPE_STRING;

/**
 * Created by zhanglei28 on 2019/3/26.
 */
public class TestSubObjSerializer implements Serializer<TestSubObj> {
    private static BreezeType<Map<String, String>> mapBreezeType;

    static {
        try {
            mapBreezeType = Breeze.getBreezeType(TestSubObj.class, "map");
        } catch (BreezeException ignore) {
        }
    }

    String[] names = new String[]{TestSubObj.class.getName(), "TestSubObj"};

    @Override
    public void writeToBuf(TestSubObj obj, BreezeBuffer buffer) throws BreezeException {
        BreezeWriter.writeMessage(buffer, () -> {
            TYPE_INT32.writeMessageField(buffer, 1, obj.getAnInt());
            TYPE_STRING.writeMessageField(buffer, 2, obj.getString());
            mapBreezeType.writeMessageField(buffer, 3, obj.getMap());
        });
    }

    @Override
    public TestSubObj readFromBuf(BreezeBuffer buffer) throws BreezeException {
        TestSubObj tso = new TestSubObj();
        BreezeReader.readMessage(buffer, (int index) -> {
            switch (index) {
                case 1:
                    tso.setAnInt(TYPE_INT32.read(buffer));
                    break;
                case 2:
                    tso.setString(TYPE_STRING.read(buffer));
                    break;
                case 3:
                    tso.setMap(mapBreezeType.read(buffer));
                    break;
            }
        });
        return tso;
    }

    @Override
    public String[] getNames() {
        return names;
    }
}
