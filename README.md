# Breeze
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/weibreeze/breeze/blob/master/LICENSE)
[![Maven Central](https://img.shields.io/maven-central/v/com.weibo/breeze.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:com.weibo%20AND%20breeze)
[![Build Status](https://img.shields.io/travis/weibreeze/breeze/master.svg?label=Build)](https://travis-ci.org/weibreeze/breeze)

# 概述
[Breeze](https://github.com/weibreeze/breeze)是一个跨语言序列化协议与服务描述schema，与protobuf类似，但更加易用并且提供对旧对象的兼容能力。

# 功能
- 支持多语言。提供[Java](https://github.com/weibreeze/breeze), [Golang](https://github.com/weibreeze/breeze-go), [PHP](https://github.com/weibreeze/breeze-php), [CPP](https://github.com/weibreeze/breeze-cpp)等版本实现。
- 提供[Breeze Generator](https://github.com/weibreeze/breeze-generator)可以自动生成多语言代码。
- 高性能、序列化结果更小。
- 简单易用。
- 对已经存在的bean对象提供更好的兼容性，可以直接进行序列化，不需要替换为breeze自动生成的对象。

# 快速入门
1. 添加依赖

```xml
        <dependency>
            <groupId>com.weibo</groupId>
            <artifactId>breeze-core</artifactId>
            <version>RELEASE</version>
        </dependency>
```
    
2. 基础类型编解码

```java
        //编码
        BreezeBuffer buffer = new BreezeBuffer(256);// 设置合适的buffer大小
        String str = "just test";
        BreezeWriter.writeString(buffer, str);
        buffer.flip();
        byte[] result = buffer.getBytes();

        //解码
        BreezeBuffer newBuffer = new BreezeBuffer(result);
        String newStr = BreezeReader.readString(newBuffer);
        System.out.println(newStr);
```

3. 集合类型编解码

```java
        //编码
        BreezeBuffer buffer = new BreezeBuffer(256);// 设置合适的buffer大小
        List<String> list = new ArrayList<>();
        list.add("s1");
        list.add("s2");
        BreezeWriter.writeCollection(buffer, list);
        buffer.flip();
        byte[] result = buffer.getBytes();

        //解码
        BreezeBuffer newBuffer = new BreezeBuffer(result);
        List<String> newList = new ArrayList<>();
        BreezeReader.readCollection(newBuffer, newList, String.class);
        System.out.println(newList.get(0));
```

4. 类型嵌套编解码

```java
        //编码
        BreezeBuffer buffer = new BreezeBuffer(256);// 设置合适的buffer大小
        List<String> list = new ArrayList<>();
        list.add("s1");
        list.add("s2");
        Map<Integer, List<String>> map = new HashMap<>();
        map.put(12, list);
        BreezeWriter.writeMap(buffer, map);
        buffer.flip();
        byte[] result = buffer.getBytes();

        //解码
        BreezeBuffer newBuffer = new BreezeBuffer(result);
        Map<Integer, List<String>> newMap = new HashMap<>();
        BreezeReader.readMap(newBuffer, newMap, String.class, List.class);// 泛型类型建议使用ParameterizedType作为参数。
        System.out.println(newMap.get(12).get(0));
```

5. Breeze Message编解码

```java
        //编码
        BreezeBuffer buffer = new BreezeBuffer(256);// 设置合适的buffer大小
        TestMsg msg = BreezeRWTest.getDefaultTestMsg(); // BreezeRWTest at https://github.com/weibreeze/breeze/blob/master/breeze-core/src/test/java/com/weibo/breeze/BreezeRWTest.java
        BreezeWriter.writeObject(buffer, msg);
        buffer.flip();
        byte[] result = buffer.getBytes();

        //解码
        BreezeBuffer newBuffer = new BreezeBuffer(result);
        TestMsg newMsg = BreezeReader.readObject(newBuffer, TestMsg.class);
        System.out.println(newMsg.getMyString());
```

6. Object编解码

```java
        //编码
        BreezeBuffer buffer = new BreezeBuffer(256);// 设置合适的buffer大小
        TestObj obj = BreezeRWTest.getDefaultTestObj();
        BreezeWriter.writeObject(buffer, obj);
        buffer.flip();
        byte[] result = buffer.getBytes();

        //解码
        BreezeBuffer newBuffer = new BreezeBuffer(result);
        TestObj newObj = BreezeReader.readObject(newBuffer, TestObj.class);
        System.out.println(newObj.getString());
```
# 使用Breeze Schema生成Message类

参见[breeze-generator](https://github.com/weibreeze/breeze-generator)

# 文档

参见[Wiki](https://github.com/weibreeze/breeze/wiki/zh_overview)