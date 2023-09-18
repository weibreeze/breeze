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

package com.weibo.breeze;

import com.weibo.breeze.message.Schema;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author zhanglei28
 * @date 2019/5/21.
 */
public class SchemaUtil {
    private static final Pattern fieldPattern = Pattern.compile("^([\\w<>., ]+) +(\\w+) *= *(\\d+) *;$");

    public static Schema parseSchema(String content) throws BreezeException {
        try {
            Schema schema = new Schema();
            BufferedReader reader = new BufferedReader(new StringReader(content));
            String packageString = null;
            String line;
            boolean inMessage = false;
            boolean inEnum = false;
            while (true) {
                line = reader.readLine();
                if (line == null) {
                    throw new BreezeException("unexpected end with schema");
                }
                line = line.trim();
                if (line.startsWith("message ")) {
                    schema.setName(packageString + "." + getName(line, 8));
                    inMessage = true;
                } else if (inMessage) {
                    if (line.equals("{")) {
                        continue;
                    }
                    if (line.startsWith("}")) {
                        return schema;
                    }
                    Matcher matcher = fieldPattern.matcher(line);
                    if (!matcher.find()) {
                        throw new BreezeException("wrong field format. line:" + line);
                    }
                    schema.putField(Integer.parseInt(matcher.group(3)), matcher.group(2).trim(), matcher.group(1).trim());
                } else if (line.startsWith("package ")) {
                    if (packageString == null) {
                        packageString = line.substring(8, line.length() - 1).trim();
                    }
                } else if (line.startsWith("option java_package")) {
                    packageString = getPackageName(line);
                } else if (line.startsWith("option java_name")) {
                    schema.setJavaName(getPackageName(line));
                } else if (line.startsWith("enum ")) {
                    schema.setName(packageString + "." + getName(line, 5));
                    inEnum = true;
                    schema.setEnum(true);
                } else if (inEnum) {
                    if (line.equals("{")) {
                        continue;
                    }
                    if (line.startsWith("}")) {
                        return schema;
                    }
                    String[] strings = line.split("=");
                    if (strings.length != 2) {
                        throw new BreezeException("wrong enum format. line:" + line);
                    }
                    schema.addEnumValue(Integer.parseInt(strings[1].substring(0, strings[1].length() - 1).trim()), strings[0].trim());
                }
            }
        } catch (IOException ignore) {
        }
        return null;
    }

    private static String getName(String line, int startPos) {
        int index = line.indexOf("(");
        if (index < 0) {
            index = line.indexOf("{");
        }
        if (index < 0) {
            index = line.length();
        }
        return line.substring(startPos, index).trim();
    }

    public static String toFileContent(Schema schema) {

        String packageName = schema.getName();
        int index = schema.getName().lastIndexOf(".");
        if (index > -1) {
            packageName = schema.getName().substring(0, index);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("option java_package = ").append(packageName).append(";\n");
        if (schema.getJavaName() != null) {
            sb.append("option java_name = ").append(schema.getJavaName()).append(";\n");
        }
        sb.append("\npackage ").append(packageName).append(";\n\n");
        if (schema.isEnum()) { // enum
            sb.append("enum ").append(schema.getName().substring(index + 1)).append("{\n");
            for (Map.Entry<Integer, String> entry : schema.getEnumValues().entrySet()) {
                sb.append("    ").append(entry.getValue()).append(" = ").append(entry.getKey()).append(";\n");
            }
        } else { // message
            sb.append("message ").append(schema.getName().substring(index + 1)).append("{\n");
            List<Integer> fields = new ArrayList<>(schema.getFields().keySet());
            Collections.sort(fields);
            for (Integer fieldIndex : fields) {
                Schema.Field field = schema.getFieldByIndex(fieldIndex);
                sb.append("    ").append(field.getType()).append(" ").append(field.getName()).append(" = ").append(field.getIndex()).append(";\n");
            }
        }
        sb.append("}\n");
        return sb.toString();
    }

    public static void checkCompatible(Schema s1, Schema s2) throws BreezeException {
        if (!s1.getName().equals(s2.getName())) {
            throw new BreezeException("breeze field name not compatible with old version. schema:" + s1.getName());
        }
        if (s1.isEnum()) {
            Map<Integer, String> values1 = s1.getEnumValues();
            Map<Integer, String> values2 = s2.getEnumValues();
            for (Map.Entry<Integer, String> entry : values1.entrySet()) {
                if (values2.get(entry.getKey()) != null && !values2.get(entry.getKey()).equals(entry.getValue())) {
                    throw new BreezeException("breeze enum value not compatible with old version. schema:" + s1.getName() + ", enum:" + entry.getValue() + ", index:" + entry.getKey() + ", old value:" + values2.get(entry.getKey()));
                }
            }
        } else {// message
            for (Schema.Field field : s1.getFields().values()) {
                Schema.Field otherField = s2.getFieldByName(field.getName());
                if (otherField != null && otherField.getIndex() != field.getIndex()) {
                    throw new BreezeException("breeze field index not compatible with old version. schema:" + s1.getName() + ", field:" + field.getName() + ", index:" + field.getIndex() + ", old index:" + otherField.getIndex());
                }
            }
        }
    }

    private static String getPackageName(String line) {
        return line.substring(line.indexOf("=") + 1, line.length() - 1).trim();
    }
}
