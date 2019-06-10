package com.weibo.breeze;

import com.weibo.breeze.message.Schema;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by zhanglei28 on 2019/5/23.
 */
public class SchemaUtilTest {
    private static final String CONTENT_1 = "option java_package = test.schema;\n\n"
            + "package test.schema;\n\n"
            + "message Test{\n"
            + "    string naelkj = 1;\n"
            + "    int32 Tyew = 2;\n"
            + "}\n";
    private static final String CONTENT_2 = "option java_package = test.schema;\n\n"
            + "package test.schema;\n\n"
            + "enum TestEnum{\n"
            + "    sdjoe = 12;\n"
            + "    sjkoie = 34;\n"
            + "}\n";

    @Test
    public void testParseSchema() throws Exception {
        // test message schema
        Schema messageSchema = Schema.newSchema("test.schema.Test")
                .putField(1, "naelkj", "string")
                .putField(2, "Tyew", "int32");
        String content = SchemaUtil.toFileContent(messageSchema);
        assertEquals(content, CONTENT_1);

        Schema result = SchemaUtil.parseSchema(content);
        assertTrue(check(messageSchema, result));

        // test enum schema
        Schema enumSchema = Schema.newSchema("test.schema.TestEnum")
                .addEnumValue(12, "sdjoe")
                .addEnumValue(34, "sjkoie");
        enumSchema.setEnum(true);
        content = SchemaUtil.toFileContent(enumSchema);
        assertEquals(content, CONTENT_2);
        result = SchemaUtil.parseSchema(content);
        assertTrue(check(enumSchema, result));
    }

    private boolean check(Schema schema1, Schema schema2) {
        return schema1.getName().equals(schema2.getName()) && schema1.getFields().size() == schema2.getFields().size()
                && schema1.getEnumValues().size() == schema2.getEnumValues().size();
    }

}