package com.weibo.breeze.serializer;

import com.weibo.breeze.BreezeBuffer;
import com.weibo.breeze.BreezeException;
import com.weibo.breeze.BreezeReader;
import com.weibo.breeze.BreezeWriter;

import java.math.BigDecimal;

/**
 * Created by zhanglei28 on 2019/3/29.
 */
public class BigDecimalSerializer implements Serializer<BigDecimal> {

    @Override
    public void writeToBuf(BigDecimal obj, BreezeBuffer buffer) throws BreezeException {
        BreezeWriter.writeMessage(buffer, BigDecimal.class.getName(), () -> BreezeWriter.writeMessageField(buffer, 1, obj.toString()));
    }

    @Override
    public BigDecimal readFromBuf(BreezeBuffer buffer) throws BreezeException {
        BigDecimal[] objects = new BigDecimal[1];
        BreezeReader.readMessage(buffer, true, (int index) -> {
            switch (index) {
                case 1:
                    objects[0] = new BigDecimal(BreezeReader.readString(buffer));
                    break;
            }
        });
        return objects[0];
    }

    @Override
    public String[] getNames() {
        return new String[]{BigDecimal.class.getName()};
    }
}
