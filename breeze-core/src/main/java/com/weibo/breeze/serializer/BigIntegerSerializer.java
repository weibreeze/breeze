package com.weibo.breeze.serializer;

import com.weibo.breeze.BreezeBuffer;
import com.weibo.breeze.BreezeException;
import com.weibo.breeze.BreezeReader;
import com.weibo.breeze.BreezeWriter;

import java.math.BigInteger;

/**
 * Created by zhanglei28 on 2019/6/4.
 */
public class BigIntegerSerializer implements Serializer<BigInteger> {
    @Override
    public void writeToBuf(BigInteger obj, BreezeBuffer buffer) throws BreezeException {
        BreezeWriter.writeMessage(buffer, BigInteger.class.getName(), () -> BreezeWriter.writeMessageField(buffer, 1, obj.toString()));
    }

    @Override
    public BigInteger readFromBuf(BreezeBuffer buffer) throws BreezeException {
        BigInteger[] objects = new BigInteger[1];
        BreezeReader.readMessage(buffer, (int index) -> {
            switch (index) {
                case 1:
                    objects[0] = new BigInteger(BreezeReader.readString(buffer, true));
                    break;
            }
        });
        return objects[0];
    }

    @Override
    public String[] getNames() {
        return new String[]{BigInteger.class.getName()};
    }
}
