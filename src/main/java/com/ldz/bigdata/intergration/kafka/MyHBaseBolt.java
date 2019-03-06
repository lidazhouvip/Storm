package com.ldz.bigdata.intergration.kafka;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.Map;

/**
 * @Author: Dazhou Li
 * @Description:
 * @CreateDate: 2019/3/5 0005 18:23
 */
public class MyHBaseBolt extends BaseRichBolt {

    private Connection connection;
    private Table table;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.rootdir", "hdfs://172.20.21.100:8020/hbase");
        config.set("hbase.zookeeper.quorum", "172.20.21.100:2181,172.20.21.101:2181,172.20.21.102:2181,172.20.21.103:2181");
        try {
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf("myTest"));
        } catch (IOException e) {
            //do something to handle exception
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        long time = tuple.getLongByField("time");
        String longitude = tuple.getStringByField("longitude");
        String latitude = tuple.getStringByField("latitude");
        try {
            Put put = new Put(Bytes.toBytes(time));
            put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("longitude"),Bytes.toBytes(longitude));
            put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("latitude"),Bytes.toBytes(latitude));
            table.put(put);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}