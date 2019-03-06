package com.ldz.bigdata.intergration.kafka;

import com.google.common.collect.Maps;
import com.ldz.bigdata.intergration.hbase.LocalWordCountHbaseStormTopology;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @Author: Dazhou Li
 * @Description:kafka整合storm
 * @CreateDate: 2019/2/18 0018 19:11
 */
public class StormKafkaTopo {

    public static void main(String[] args) {

        Config config = new Config();
        HashMap<String, Object> hbaseConf = new HashMap<>();
        hbaseConf.put("hbase.rootdir", "hdfs://172.20.21.100:8020/hbase");
        //hbaseConf.put("hbase.zookeeper.quorum", "172.20.21.100:2181");
        config.put("hbase.conf", hbaseConf);

        //kafka使用的zk地址
        BrokerHosts hosts = new ZkHosts("172.20.21.100:2181");
        //kafka存储数据的topic地址
        String topic = "project_topic";
        //指定zk的一个根目录，存储KafkaSpout读取数据的位置信息（offset）
        String zkRoot = "/" + topic;
        String id = UUID.randomUUID().toString();
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, id);

        //设置读取偏移量的操作
        spoutConfig.startOffsetTime=kafka.api.OffsetRequest.LatestTime();

        TopologyBuilder builder = new TopologyBuilder();

        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        String SPOUT_ID=KafkaSpout.class.getSimpleName();
        builder.setSpout(SPOUT_ID,kafkaSpout);

        String BOLT_ID=LogProcessBolt.class.getSimpleName();
        builder.setBolt(BOLT_ID,new LogProcessBolt()).shuffleGrouping(SPOUT_ID);

        builder.setBolt("HBaseBolt", new MyHBaseBolt()).shuffleGrouping(BOLT_ID);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(StormKafkaTopo.class.getSimpleName(),
                config,
                builder.createTopology());
    }
}
