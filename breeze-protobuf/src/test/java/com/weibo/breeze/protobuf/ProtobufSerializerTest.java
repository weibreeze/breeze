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
    @Test
    public void testProtobuf() throws Exception {
        Breeze.registerSerializer(new ProtobufSerializer(User.class));
        Breeze.registerSerializer(new ProtobufSerializer(Address.class));

        Address address = Address.newBuilder().setAddress("sdfhskj").setId(2134).putOther("jkl", "heury").putOther("ere", "eruow").build();
        User user = User.newBuilder().setAge(16).setName("test").setGender(Gender.Woman).addAddress(address).build();
        User user1 = testSerialize(user, User.class);
        assertEquals(user, user1);

    }

    protected static <T> T testSerialize(Object object, Class<T> clz) throws BreezeException {
        BreezeBuffer buffer = new BreezeBuffer(256);
        BreezeWriter.writeObject(buffer, object);
        buffer.flip();
        byte[] result = buffer.getBytes();
        BreezeBuffer newBuffer = new BreezeBuffer(result);
        return BreezeReader.readObject(newBuffer, clz);
    }
}