package com.coocaa.demo;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapBuilder;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 数据变更记录属性配置
 */
@Data
@ConfigurationProperties(prefix = "common.cdc")
@Component
public class FlinkCdcProperties implements Serializable {

    private static final long serialVersionUID = 1L;

    // default 3306 port
    private Integer port = 3306;

    private String hostname;

    private String username;

    private String password;

    private List<Item> itemList;

    @Data
    private static class Item {

        /**
         * 数据库，精确匹配
         */
        private String database;

        /**
         * 表名，多个用逗号隔开
         */
        private String tableNames;

        /**
         * 忽略的字段，多个用逗号隔开
         */
        private String ignoreColumns;
    }

    /**
     * 获取 库名
     */
    public String[] getDatabases() {
        if (CollUtil.isEmpty(itemList)) {
            return new String[0];
        }
        return itemList.stream().map(Item::getDatabase).distinct().toArray(String[]::new);
    }

    /**
     * 获取 全限定表名 database.table
     */
    public String[] getTables() {
        if (CollUtil.isEmpty(itemList)) {
            return new String[0];
        }
        List<String> tableList = new ArrayList<>();
        for (Item item : itemList) {
            final String tableNames = item.getTableNames();
            if (StrUtil.isBlank(tableNames)) {
                continue;
            }

            final String[] tableNameArray = tableNames.split(",");
            for (String tableName : tableNameArray) {
                tableList.add(StrUtil.format("{}.{}", item.getDatabase(), tableName));
            }
        }
        return tableList.stream().distinct().toArray(String[]::new);
    }

    /**
     * 获取全限定的表名到忽略字段集合的映射
     */
    public Map<String, Set<String>> getIgnoreColumnMap() {
        if (CollUtil.isEmpty(itemList)) {
            return MapUtil.empty();
        }
        final MapBuilder<String, Set<String>> builder = MapUtil.builder();
        for (Item item : itemList) {
            final String tableNames = item.getTableNames();
            final String ignoreColumns = item.getIgnoreColumns();
            if (StrUtil.isBlank(tableNames) || StrUtil.isBlank(ignoreColumns)) {
                continue;
            }

            final String[] tableNameArray = tableNames.split(",");
            for (String tableName : tableNameArray) {
                // 全限定表名
                final String key = StrUtil.format("{}.{}", item.getDatabase(), tableName);

                // 忽略的字段
                final Set<String> value = Arrays.stream(ignoreColumns.split(",")).collect(Collectors.toSet());
                builder.put(key, value);
            }
        }
        return builder.build();
    }
}