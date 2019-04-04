package com.weibo.breeze.test.obj;

import java.util.List;

/**
 * Created by zhanglei28 on 2019/3/21.
 */
public class TestObj {
    private TestSubObj subObj;
    private Integer integer;
    private String string;
    private List<TestSubObj> list;

    public String baseString; // not serialize in TestObjSerializer, but in CommonSerializer

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TestObj testObj = (TestObj) o;

        if (subObj != null ? !subObj.equals(testObj.subObj) : testObj.subObj != null) return false;
        if (integer != null ? !integer.equals(testObj.integer) : testObj.integer != null) return false;
        if (string != null ? !string.equals(testObj.string) : testObj.string != null) return false;
        if (list != null ? !list.equals(testObj.list) : testObj.list != null) return false;
        return baseString != null ? baseString.equals(testObj.baseString) : testObj.baseString == null;
    }

    @Override
    public int hashCode() {
        int result = subObj != null ? subObj.hashCode() : 0;
        result = 31 * result + (integer != null ? integer.hashCode() : 0);
        result = 31 * result + (string != null ? string.hashCode() : 0);
        result = 31 * result + (list != null ? list.hashCode() : 0);
        result = 31 * result + (baseString != null ? baseString.hashCode() : 0);
        return result;
    }
}
