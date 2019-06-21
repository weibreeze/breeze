package com.weibo.breeze.test.serializer;

import com.weibo.breeze.BreezeBuffer;
import com.weibo.breeze.BreezeException;
import com.weibo.breeze.BreezeReader;
import com.weibo.breeze.BreezeWriter;
import com.weibo.breeze.serializer.Serializer;
import com.weibo.breeze.test.obj.TestObj;
import com.weibo.breeze.test.obj.TestSubObj;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhanglei28 on 2019/3/27.
 */
public class TestObjSerializer implements Serializer<TestObj> {
    String[] names = new String[]{TestObj.class.getName(), "TestObj"};

    @Override
    public void writeToBuf(TestObj obj, BreezeBuffer buffer) throws BreezeException {
        BreezeWriter.writeMessage(buffer, TestObj.class.getName(), () -> {
            BreezeWriter.writeMessageField(buffer, 1, obj.getSubObj());
            BreezeWriter.writeMessageField(buffer, 2, obj.getString());
            BreezeWriter.writeMessageField(buffer, 3, obj.getInteger());
            BreezeWriter.writeMessageField(buffer, 4, obj.getList());
        });
    }

    @Override
    public TestObj readFromBuf(BreezeBuffer buffer) throws BreezeException {
        TestObj to = new TestObj();
        BreezeReader.readMessage(buffer, (int index) -> {
            switch (index) {
                case 1:
                    to.setSubObj(BreezeReader.readObject(buffer, TestSubObj.class));
                    break;
                case 2:
                    to.setString(BreezeReader.readString(buffer, true));
                    break;
                case 3:
                    to.setInteger(BreezeReader.readInt32(buffer, true));
                    break;
                case 4:
                    List<TestSubObj> list = new ArrayList<>();
                    BreezeReader.readCollection(buffer, list, TestSubObj.class);
                    to.setList(list);
                    break;
            }
        });
        return to;
    }

    @Override
    public String[] getNames() {
        return names;
    }

}
