package com.coocaa.demo;

import cn.hutool.extra.spring.SpringUtil;
import com.alibaba.fastjson.JSONObject;

import java.util.Map;
import java.util.Objects;

/**
 * @author lilou
 * @since 2023/4/25 18:02
 */
public class FlinkConvertorFactory {

    public static JSONObject convertToObj(FlinkCdcModel model) {
        final String dbTable = model.getDbTable();
        FlinkCdcConvertor flinkCdcConvertor = getDatarecordConvertor(dbTable);
        if (Objects.isNull(flinkCdcConvertor)) {
            return null;
        }

        return flinkCdcConvertor.convert(model);
    }

    private static FlinkCdcConvertor getDatarecordConvertor(String dbTable) {
        // 从容器中获取
        final Map<String, FlinkCdcConvertor> convertorMap = SpringUtil.getBeansOfType(FlinkCdcConvertor.class);
        for (FlinkCdcConvertor convertor : convertorMap.values()) {
            // 找到第一个可以处理的转换器，返回
            if (convertor.isMatch(dbTable)) {
                return convertor;
            }
        }
        return null;
    }


}
