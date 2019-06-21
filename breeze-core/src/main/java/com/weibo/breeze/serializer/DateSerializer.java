package com.weibo.breeze.serializer;

import com.weibo.breeze.BreezeBuffer;
import com.weibo.breeze.BreezeException;
import com.weibo.breeze.BreezeReader;
import com.weibo.breeze.BreezeWriter;

import java.util.Date;

/**
 * Created by zhanglei28 on 2019/3/28.
 */
public class DateSerializer implements Serializer<Date> {
    @Override
    public void writeToBuf(Date date, BreezeBuffer buffer) throws BreezeException {
        BreezeWriter.writeMessage(buffer, Date.class.getName(), () -> BreezeWriter.writeMessageField(buffer, 1, date.getTime()));
    }

    @Override
    public Date readFromBuf(BreezeBuffer buffer) throws BreezeException {
        Date date = new Date(0);
        BreezeReader.readMessage(buffer, (int index) -> {
            switch (index) {
                case 1:
                    date.setTime(BreezeReader.readInt64(buffer, true));
                    break;
            }
        });
        return date;
    }

    @Override
    public String[] getNames() {
        return new String[]{Date.class.getName()};
    }
}
