package com.coocaa.demo;

import com.alibaba.fastjson.JSONObject;

public interface FlinkCdcConvertor {

    /**
     * 是否匹配，根据 【库名.表名】可以唯一确定一个策略
     *
     * @param dbTable 库名.表名
     * @return 结果
     */
    boolean isMatch(String dbTable);

    /**
     * 转换为 JSONObject 对象，方便下游处理
     *
     * @param model 数据模型
     * @return 结果
     */
    JSONObject convert(FlinkCdcModel model);
}