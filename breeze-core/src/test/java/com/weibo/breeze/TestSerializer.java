package com.weibo.breeze;

import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

import static org.junit.Assert.assertEquals;

/**
 * Created by zhanglei28 on 2019/3/29.
 */
public class TestSerializer {

    @Test
    public void testDate() throws Exception {
        Date date = new Date(23424587);
        Date date1 = BreezeRWTest.testSerialize(date, Date.class);
        assertEquals(date.getTime(), date1.getTime());
    }

    @Test
    public void testBigDecimal() throws Exception {
        BigDecimal bigDecimal = new BigDecimal("729834792837498237498234723984239842394");
        BigDecimal bigDecimal1 = BreezeRWTest.testSerialize(bigDecimal, BigDecimal.class);
        assertEquals(bigDecimal.toString(), bigDecimal1.toString());
    }

    @Test
    public void testTimestamp() throws Exception {
        Timestamp timestamp = new Timestamp(2134);
        Timestamp timestamp1 = BreezeRWTest.testSerialize(timestamp, Timestamp.class);
        assertEquals(timestamp.getTime(), timestamp1.getTime());
    }
}
