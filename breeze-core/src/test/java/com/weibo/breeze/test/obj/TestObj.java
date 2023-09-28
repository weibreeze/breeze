package com.weibo.breeze.test.obj;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Created by zhanglei28 on 2019/3/21.
 */
public class TestObj {
    public String baseString; // not serialize in TestObjSerializer, but in CommonSerializer
    private TestSubObj subObj;
    private Integer integer;
    private String string;
    private List<TestSubObj> list;
    private int[] intArray;
    private String[] stringArray;
    private TestSubObj[] objArray;

    public TestSubObj getSubObj() {
        return subObj;
    }

    public void setSubObj(TestSubObj subObj) {
        this.subObj = subObj;
    }

    public Integer getInteger() {
        return integer;
    }

    public void setInteger(Integer integer) {
        this.integer = integer;
    }

    public String getString() {
        return string;
    }

    public void setString(String string) {
        this.string = string;
    }

    public List<TestSubObj> getList() {
        return list;
    }

    public void setList(List<TestSubObj> list) {
        this.list = list;
    }

    public int[] getIntArray() {
        return intArray;
    }

    public void setIntArray(int[] intArray) {
        this.intArray = intArray;
    }

    public String[] getStringArray() {
        return stringArray;
    }

    public void setStringArray(String[] stringArray) {
        this.stringArray = stringArray;
    }

    public TestSubObj[] getObjArray() {
        return objArray;
    }

    public void setObjArray(TestSubObj[] objArray) {
        this.objArray = objArray;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestObj testObj = (TestObj) o;
        return Objects.equals(baseString, testObj.baseString) && Objects.equals(subObj, testObj.subObj) && Objects.equals(integer, testObj.integer) && Objects.equals(string, testObj.string) && Objects.equals(list, testObj.list) && Arrays.equals(intArray, testObj.intArray) && Arrays.equals(stringArray, testObj.stringArray) && Arrays.equals(objArray, testObj.objArray);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(baseString, subObj, integer, string, list);
        result = 31 * result + Arrays.hashCode(intArray);
        result = 31 * result + Arrays.hashCode(stringArray);
        result = 31 * result + Arrays.hashCode(objArray);
        return result;
    }
}
