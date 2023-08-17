package com.coocaa.demo;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import lombok.Data;

import java.util.Objects;

@Data
public class FlinkCdcModel {
    private JSONObject before;
    private JSONObject after;
    private JSONObject source;
    private String op;
    private Long tsMs;

    public String getDbTable() {
        final String db = source.getString("db");
        final String table = source.getString("table");
        return StrUtil.format("{}.{}", db, table);
    }

    public String getChangeType() {
        switch (op) {
            case "u":
                return "更新";
            case "c":
                return "添加";
            case "d":
                return "删除";

        }
        return "";
    }

    public String getDiffObjStr() {
        return JSON.toJSONString(getDiffObj(), SerializerFeature.SortField);
    }

    public JSONObject getDiffObj() {
        final JSONObject object = new JSONObject(true);
        object.put("before", before);
        object.put("after", after);
        return object;
    }

    public Object getFromAny(String key) {
        if (Objects.nonNull(before)) {
            return before.get(key);
        }
        if (Objects.nonNull(after)) {
            return after.get(key);
        }
        return null;
    }

    public Object getFromBefore(String key) {
        if (Objects.nonNull(before)) {
            return before.get(key);
        }
        return null;
    }

    public Object getFromAfter(String key) {
        if (Objects.nonNull(after)) {
            return after.get(key);
        }
        return null;
    }

}