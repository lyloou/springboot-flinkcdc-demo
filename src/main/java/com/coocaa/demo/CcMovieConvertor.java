package com.coocaa.demo;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSONObject;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class CcMovieConvertor implements FlinkCdcConvertor {
    public interface KEY {
        String record_date = "record_date";
        String table_name = "table_name";
        String table_id = "table_id";
        String coocaa_v_id = "coocaa_v_id";
        String third_v_id = "third_v_id";
        String coocaa_vu_id = "coocaa_vu_id";
        String third_vu_id = "third_vu_id";
        String third_source = "third_source";
        String pay_type = "pay_type";
        String resource_type = "resource_type";
        String license = "license";
        String record_data = "record_data";
    }

    @Override
    public boolean isMatch(String dbTable) {
        return dbTable.startsWith("cc_media_source.cc_movie_2018_");
    }

    @Override
    public JSONObject convert(FlinkCdcModel model) {
        final JSONObject obj = new JSONObject(true);
        obj.put(KEY.record_date, DateUtil.formatDateTime(new Date(model.getTsMs())));
        obj.put(KEY.table_name, model.getDbTable());
        obj.put(KEY.table_id, model.getFromAny("m_id"));
        obj.put(KEY.coocaa_v_id, model.getFromAny("coocaa_v_id"));
        obj.put(KEY.third_v_id, "");
        obj.put(KEY.coocaa_vu_id, model.getFromAny("coocaa_m_id"));
        obj.put(KEY.third_vu_id, "");
        obj.put(KEY.third_source, model.getFromAfter("source"));
        obj.put(KEY.pay_type, "");
        obj.put(KEY.resource_type, "");
        obj.put(KEY.license, "");
        obj.put(KEY.record_data, model.getDiffObj());
        return obj;
    }

}