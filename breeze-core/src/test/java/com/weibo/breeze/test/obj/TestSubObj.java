package com.weibo.breeze.test.obj;

import java.util.Map;

/**
 * Created by zhanglei28 on 2019/3/21.
 */
public class TestSubObj {
    private int anInt;
    private String string;
    private Map<String, String> map;

    public int getAnInt() {
        return anInt;
    }

    public void setAnInt(int anInt) {
        this.anInt = anInt;
    }

    public String getString() {
        return string;
    }

    public void setString(String string) {
        this.string = string;
    }

    public Map<String, String> getMap() {
        return map;
    }

    public void setMap(Map<String, String> map) {
        this.map = map;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TestSubObj that = (TestSubObj) o;

        if (anInt != that.anInt) return false;
        if (string != null ? !string.equals(that.string) : that.string != null) return false;
        return map != null ? map.equals(that.map) : that.map == null;
    }

    @Override
    public int hashCode() {
        int result = anInt;
        result = 31 * result + (string != null ? string.hashCode() : 0);
        result = 31 * result + (map != null ? map.hashCode() : 0);
        return result;
    }
}
