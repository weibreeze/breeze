package com.weibo.breeze.test.serializer;

import com.weibo.breeze.*;
import com.weibo.breeze.serializer.Serializer;
import com.weibo.breeze.test.obj.TestObj;
import com.weibo.breeze.test.obj.TestSubObj;
import com.weibo.breeze.type.BreezeType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.weibo.breeze.type.Types.TYPE_INT32;
import static com.weibo.breeze.type.Types.TYPE_STRING;

/**
 * Created by zhanglei28 on 2019/3/27.
 */
@SuppressWarnings("unchecked")
public class TestObjSerializer implements Serializer<TestObj> {
    private static BreezeType<TestSubObj> subObjBreezeType;
    private static BreezeType<List<TestSubObj>> listBreezeType;
    private static BreezeType<List<Integer>> intArrayBreezeType;
    private static BreezeType<List<String>> stringArrayBreezeType;
    private static BreezeType<List<TestSubObj>> objArrayBreezeType;

    static {
        try {
            subObjBreezeType = Breeze.getBreezeType(TestSubObj.class);
            listBreezeType = Breeze.getBreezeType(TestObj.class, "list");
            intArrayBreezeType = Breeze.getBreezeType(TestObj.class, "intArray");
            stringArrayBreezeType = Breeze.getBreezeType(TestObj.class, "stringArray");
            objArrayBreezeType = Breeze.getBreezeType(TestObj.class, "objArray");
        } catch (BreezeException ignore) {
        }
    }

    String[] names = new String[]{TestObj.class.getName(), "TestObj"};

    @Override
    public void writeToBuf(TestObj obj, BreezeBuffer buffer) throws BreezeException {
        BreezeWriter.writeMessage(buffer, () -> {
            subObjBreezeType.writeMessageField(buffer, 1, obj.getSubObj());
            TYPE_STRING.writeMessageField(buffer, 2, obj.getString());
            TYPE_INT32.writeMessageField(buffer, 3, obj.getInteger());
            listBreezeType.writeMessageField(buffer, 4, obj.getList());
            intArrayBreezeType.writeMessageField(buffer, 5, Arrays.stream(obj.getIntArray()).boxed().collect(Collectors.toList()));
            stringArrayBreezeType.writeMessageField(buffer, 6, Arrays.asList(obj.getStringArray()));
            objArrayBreezeType.writeMessageField(buffer, 7, Arrays.asList(obj.getObjArray()));
        });
    }

    @Override
    public TestObj readFromBuf(BreezeBuffer buffer) throws BreezeException {
        TestObj to = new TestObj();
        BreezeReader.readMessage(buffer, (int index) -> {
            switch (index) {
                case 1:
                    to.setSubObj(subObjBreezeType.read(buffer));
                    break;
                case 2:
                    to.setString(TYPE_STRING.read(buffer));
                    break;
                case 3:
                    to.setInteger(TYPE_INT32.read(buffer));
                    break;
                case 4:
                    to.setList(listBreezeType.read(buffer));
                    break;
                case 5:
                    to.setIntArray(intArrayBreezeType.read(buffer).stream().mapToInt(Integer::intValue).toArray());
                    break;
                case 6:
                    to.setStringArray(stringArrayBreezeType.read(buffer).toArray(new String[0]));
                    break;
                case 7:
                    to.setObjArray(objArrayBreezeType.read(buffer).toArray(new TestSubObj[0]));
                    break;
                default: //skip unknown field
                    BreezeReader.readObject(buffer, Object.class);
            }
        });
        return to;
    }

    @Override
    public String[] getNames() {
        return names;
    }

}
