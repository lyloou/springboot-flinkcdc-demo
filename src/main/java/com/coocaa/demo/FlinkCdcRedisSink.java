package com.coocaa.demo;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.extra.spring.SpringUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.serializer.SerializerFeature;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.*;

@Slf4j
public class FlinkCdcRedisSink extends RichSinkFunction<String> {

    private FlinkCdcProperties flinkCdcProperties;


    private synchronized FlinkCdcProperties getProperties() {
        if (Objects.isNull(flinkCdcProperties)) {
            flinkCdcProperties = SpringUtil.getBean(FlinkCdcProperties.class);
        }
        return flinkCdcProperties;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }


    // 每条记录插入时调用一次
    @Override
    public void invoke(String value, Context context) throws Exception {
        try {
            doInvoke(value);
        } catch (Exception e) {
            final String message = "[DatarecordRedisSink#invoke]: 记录数据异常，value:" + value;
            log.error(message, e);
        }
    }

    private void doInvoke(String value) {
        // value 的值
        // {
        //  "before":null,"after":{"name":"5","id":5},
        //  "source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"bigdata","sequence":null,"table":"person","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},
        //  "op":"r",
        //  "ts_ms":1682389025860,
        //  "transaction":null
        // }

        final FlinkCdcModel model = JSON.parseObject(value, FlinkCdcModel.class, Feature.OrderedField);

        // 是否过滤
        if (isIgnored(model)) {
            if (log.isDebugEnabled()) {
                log.debug("[DatarecordRedisSink#invoke]: 被忽略的更新，value:{}", value);
            }
            return;
        }

        JSONObject entity = FlinkConvertorFactory.convertToObj(model);
        if (Objects.nonNull(entity)) {
            final String data = JSON.toJSONString(entity, SerializerFeature.SortField);
            log.info("[DatarecordRedisSink#invoke]: ==> add to queue:" + data);

            // TODO
        }
    }

    private boolean isIgnored(FlinkCdcModel model) {
        final String op = model.getOp();

        // 非cud操作，过滤掉
        if (!Arrays.asList("c", "u", "d").contains(op)) {
            return true;
        }

        // 过滤掉只有忽略字段更新的数据
        if ("u".equals(op)) {

            final Map<String, Set<String>> ignoreColumnMap = getProperties().getIgnoreColumnMap();
            String dbTable = model.getDbTable();

            // 如果内容相同，只是忽略的字段发生了变更，则可以忽略
            return isSameIfIgnoreColumnRemoved(model, ignoreColumnMap, dbTable);
        }
        return false;
    }

    private boolean isSameIfIgnoreColumnRemoved(FlinkCdcModel model, Map<String, Set<String>> ignoreColumnMap, String dbTable) {
        final Set<String> ignoreColumns = ignoreColumnMap.get(dbTable);
        if (CollUtil.isEmpty(ignoreColumns)) {
            return false;
        }

        // 复制一份出来，不影响源数据
        final Map<String, Object> before = new TreeMap<>(model.getBefore());
        final Map<String, Object> after = new TreeMap<>(model.getAfter());
        for (String ignoreColumn : ignoreColumns) {
            before.remove(ignoreColumn);
            after.remove(ignoreColumn);
        }
        // 忽略字段去除后，是否还相等
        return StrUtil.equals(JSON.toJSONString(before, SerializerFeature.SortField), JSON.toJSONString(after, SerializerFeature.SortField));
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
