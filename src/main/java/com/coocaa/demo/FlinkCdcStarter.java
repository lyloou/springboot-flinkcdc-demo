package com.coocaa.demo;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Properties;

@Component
public class FlinkCdcStarter {


    @Autowired
    private FlinkCdcProperties flinkCdcProperties;

    @PostConstruct
    public void init() throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(flinkCdcProperties.getHostname())
                .port(flinkCdcProperties.getPort())
                .databaseList(flinkCdcProperties.getDatabases()) // set captured database
                .tableList(flinkCdcProperties.getTables()) // set captured table
                .username(flinkCdcProperties.getUsername())
                .password(flinkCdcProperties.getPassword())
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                // 配置参数
                .debeziumProperties(getDebeziumProperties())
                // 增量
                // .startupOptions(StartupOptions.specificOffset("mysql-bin.000004", 7988))
                .startupOptions(StartupOptions.latest())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        // env.enableCheckpointing(3000);
        // env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///tmp/flink-ck/checkpoints"));

        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks
                .setParallelism(4)
                // 通过Checkpoint机制来保证发生failure时不会丢数，实现exactly once语义
                // Note: currently, the source function can't run in multiple parallel instances.
                .addSink(new FlinkCdcRedisSink())
                .setParallelism(1); // use parallelism 1 for sink to keep message ordering
        env.executeAsync("DatarecordCdcStarter");
    }

    /**
     * 版权声明：本文为CSDN博主「zhu.hh」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
     * 原文链接：https://blog.csdn.net/qq_30529079/article/details/127809317
     */
    private static Properties getDebeziumProperties() {
        Properties properties = new Properties();
        properties.setProperty("converters", "dateConverters");
        //根据类在那个包下面修改
        properties.setProperty("dateConverters.type", MySqlDateTimeConverter.class.getName());
        properties.setProperty("dateConverters.format.date", "yyyy-MM-dd");
        properties.setProperty("dateConverters.format.time", "HH:mm:ss");
        properties.setProperty("dateConverters.format.datetime", "yyyy-MM-dd HH:mm:ss");
        properties.setProperty("dateConverters.format.timestamp", "yyyy-MM-dd HH:mm:ss");
        properties.setProperty("dateConverters.format.timestamp.zone", "UTC+8");
        properties.setProperty("debezium.snapshot.locking.mode", "none"); //全局读写锁，可能会影响在线业务，跳过锁设置
        properties.setProperty("include.schema.changes", "true");
        properties.setProperty("bigint.unsigned.handling.mode", "long");
        properties.setProperty("decimal.handling.mode", "double");
        return properties;
    }


}