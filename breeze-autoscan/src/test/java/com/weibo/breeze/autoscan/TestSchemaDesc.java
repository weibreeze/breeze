package com.weibo.breeze.autoscan;

import com.weibo.breeze.BreezeException;
import com.weibo.breeze.annotation.BreezeSchema;
import com.weibo.breeze.message.Schema;
import com.weibo.breeze.message.SchemaDesc;

/**
 * Created by zhanglei28 on 2019/4/1.
 */
@BreezeSchema
public class TestSchemaDesc implements SchemaDesc {
    @Override
    public Schema[] getSchemas() {
        try {
            return new Schema[]{Schema.newSchema(TestBean.class.getName()).putField(1,"name")};
        } catch (BreezeException ignore) {
        }
        return new Schema[0];
    }
}
