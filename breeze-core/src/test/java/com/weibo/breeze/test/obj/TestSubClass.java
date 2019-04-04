package com.weibo.breeze.test.obj;

import java.util.Map;

/**
 * Created by zhanglei28 on 2019/3/27.
 */
public class TestSubClass extends TestObj {
    private String myString;
    private Map<String, String> myMap;

    public int myInt;

    public String getMyString() {
        return myString;
    }

    public void setMyString(String myString) {
        this.myString = myString;
    }

    public Map<String, String> getMyMap() {
        return myMap;
    }

    public void setMyMap(Map<String, String> myMap) {
        this.myMap = myMap;
    }
}
