/*
 *
 *   Copyright 2019 Weibo, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.weibo.breeze.serializer;

import com.weibo.breeze.BreezeBuffer;
import com.weibo.breeze.BreezeException;
import com.weibo.breeze.BreezeReader;
import com.weibo.breeze.BreezeWriter;

import java.util.Date;

import static com.weibo.breeze.type.Types.TYPE_INT64;

/**
 * @author zhanglei28
 * @date 2019/3/28.
 */
public class DateSerializer implements Serializer<Date> {
    private static final String[] names = new String[]{Date.class.getName()};

    @Override
    public void writeToBuf(Date date, BreezeBuffer buffer) throws BreezeException {
        BreezeWriter.writeMessage(buffer, () -> TYPE_INT64.writeMessageField(buffer, 1, date.getTime()));
    }

    @Override
    public Date readFromBuf(BreezeBuffer buffer) throws BreezeException {
        Date date = new Date(0);
        BreezeReader.readMessage(buffer, (int index) -> {
            switch (index) {
                case 1:
                    date.setTime(TYPE_INT64.read(buffer));
                    break;
            }
        });
        return date;
    }

    @Override
    public String[] getNames() {
        return names;
    }
}
