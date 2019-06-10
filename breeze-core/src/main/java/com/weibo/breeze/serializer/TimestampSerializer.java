package com.weibo.breeze.serializer;

import com.weibo.breeze.BreezeBuffer;
import com.weibo.breeze.BreezeException;
import com.weibo.breeze.BreezeReader;
import com.weibo.breeze.BreezeWriter;

import java.sql.Timestamp;

/**
 * Created by zhanglei28 on 2019/4/3.
 */
public class TimestampSerializer implements Serializer<Timestamp> {
    @Override
    public void writeToBuf(Timestamp obj, BreezeBuffer buffer) throws BreezeException {
        BreezeWriter.writeMessage(buffer, Timestamp.class.getName(), () -> BreezeWriter.writeMessageField(buffer, 1, obj.getTime()));
    }

    @Override
    public Timestamp readFromBuf(BreezeBuffer buffer) throws BreezeException {
        Timestamp timestamp = new Timestamp(0);
        BreezeReader.readMessage(buffer, true, (int index) -> {
            switch (index) {
                case 1:
                    timestamp.setTime(BreezeReader.readInt64(buffer));
                    break;
            }
        });
        return timestamp;
    }

    @Override
    public String[] getNames() {
        return new String[]{Timestamp.class.getName()};
    }
}
