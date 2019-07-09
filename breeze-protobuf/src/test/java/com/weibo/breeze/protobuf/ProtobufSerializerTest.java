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

package com.weibo.breeze.protobuf;

import com.weibo.breeze.*;
import com.weibo.test.proto.Address;
import com.weibo.test.proto.Gender;
import com.weibo.test.proto.User;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by zhanglei28 on 2019/4/2.
 */
public class ProtobufSerializerTest {
    protected static <T> T testSerialize(Object object, Class<T> clz) throws BreezeException {
        BreezeBuffer buffer = new BreezeBuffer(256);
        BreezeWriter.writeObject(buffer, object);
        buffer.flip();
        byte[] result = buffer.getBytes();
        BreezeBuffer newBuffer = new BreezeBuffer(result);
        return (T) BreezeReader.readObject(newBuffer, clz);
    }

    @Test
    public void testProtobuf() throws Exception {
        Breeze.registerSerializer(new ProtobufSerializer(User.class));
        Breeze.registerSerializer(new ProtobufSerializer(Address.class));

        Address address = Address.newBuilder().setAddress("sdfhskj").setId(2134).putOther("jkl", "heury").putOther("ere", "eruow").build();
        User user = User.newBuilder().setAge(16).setName("test").setGender(Gender.Woman).addAddress(address).build();
        User user1 = testSerialize(user, User.class);
        assertEquals(user, user1);

    }
}