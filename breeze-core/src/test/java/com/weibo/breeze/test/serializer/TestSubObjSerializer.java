package com.weibo.breeze.test.serializer;

import com.weibo.breeze.BreezeBuffer;
import com.weibo.breeze.BreezeException;
import com.weibo.breeze.BreezeReader;
import com.weibo.breeze.BreezeWriter;
import com.weibo.breeze.serializer.Serializer;
import com.weibo.breeze.test.obj.TestSubObj;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhanglei28 on 2019/3/26.
 */
public class TestSubObjSerializer implements Serializer<TestSubObj> {
    String[] names = new String[]{TestSubObj.class.getName(), "TestSubObj"};

    @Override
    public void writeToBuf(TestSubObj obj, BreezeBuffer buffer) throws BreezeException {
        BreezeWriter.writeMessage(buffer, TestSubObj.class.getName(), () -> {
            BreezeWriter.writeMessageField(buffer, 1, obj.getAnInt());
            BreezeWriter.writeMessageField(buffer, 2, obj.getString());
            BreezeWriter.writeMessageField(buffer, 3, obj.getMap());
        });
    }

    @Override
    public TestSubObj readFromBuf(BreezeBuffer buffer) throws BreezeException {
        TestSubObj tso = new TestSubObj();
        BreezeReader.readMessage(buffer, (int index) -> {
            switch (index) {
                case 1:
                    tso.setAnInt(BreezeReader.readInt32(buffer, true));
                    break;
                case 2:
                    tso.setString(BreezeReader.readString(buffer, true));
                    break;
                case 3:
                    Map<String, String> map = new HashMap<>();
                    BreezeReader.readMap(buffer, map, String.class, String.class);
                    tso.setMap(map);
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
