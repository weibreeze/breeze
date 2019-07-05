package com.weibo.breeze.autoscan;

import com.weibo.breeze.BreezeBuffer;
import com.weibo.breeze.BreezeException;
import com.weibo.breeze.BreezeReader;
import com.weibo.breeze.BreezeWriter;
import com.weibo.breeze.annotation.BreezeSerializer;
import com.weibo.breeze.serializer.Serializer;

import static com.weibo.breeze.type.Types.TYPE_STRING;

/**
 * Created by zhanglei28 on 2019/4/1.
 */
@BreezeSerializer
public class TestSerializer implements Serializer<TestBean> {

    @Override
    public void writeToBuf(TestBean obj, BreezeBuffer buffer) throws BreezeException {
        BreezeWriter.writeMessage(buffer, () -> {
            BreezeWriter.writeMessageField(buffer, 1, obj.getName());
        });
    }

    @Override
    public TestBean readFromBuf(BreezeBuffer buffer) throws BreezeException {
        TestBean testBean = new TestBean();
        BreezeReader.readMessage(buffer, (int index) -> {
            switch (index) {
                case 1:
                    testBean.setName(TYPE_STRING.read(buffer));
                    break;
            }
        });
        return testBean;
    }

    @Override
    public String[] getNames() {
        return new String[]{TestBean.class.getName()};
    }

}
