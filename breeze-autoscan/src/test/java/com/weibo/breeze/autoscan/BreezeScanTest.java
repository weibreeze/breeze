package com.weibo.breeze.autoscan;

import com.weibo.breeze.Breeze;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Created by zhanglei28 on 2019/4/1.
 */
public class BreezeScanTest {
    @Test
    public void scan() throws Exception {
        assertNull(Breeze.getSerializer(TestBean.class.getName()));
        BreezeScan breezeScan = new BreezeScan("com.weibo");
        breezeScan.scan();
        assertNotNull(Breeze.getSerializer(TestBean.class.getName()));
    }

}