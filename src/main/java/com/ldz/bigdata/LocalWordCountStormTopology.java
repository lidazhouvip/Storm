package com.ldz.bigdata;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * @Author: Dazhou Li
 * @Description:使用Storm完成词频统计功能
 * @CreateDate: 2019/2/8 0008 12:07
 */
public class LocalWordCountStormTopology {
    public static class DataSourceSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;

        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        /**
         * 业务：
         * 1） 读取指定目录的文件夹下的数据:/Users/rocky/data/storm/wc
         * 2） 把每一行数据发射出去
         */
        @Override
        public void nextTuple() {
            // 获取所有文件以.txt结尾的文件，以递归的方式
            Collection<File> files = FileUtils.listFiles(new File("E:\\test"), new String[]{"txt"}, true);
            for (File file:files){
                try {
                    // 获取一个文件中的所有内容
                    List<String> lines = FileUtils.readLines(file);

                    // 获取一行的内容
                    for (String line:lines){
                        // 发射出去
                        this.collector.emit(new Values(line));
                    }
                    // TODO... 数据处理完之后，改名，否则一直重复执行
                    FileUtils.moveFile(file, new File(file.getAbsolutePath() + System.currentTimeMillis()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("line"));
        }
    }

    /**
     * 对数据进行分割
     */
    public static class SplitBolt extends BaseRichBolt{

        private OutputCollector collector;
        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
            this.collector=collector;
        }

        /**
         * 业务逻辑：
         *   line： 对line进行分割，按照逗号
         */
        @Override
        public void execute(Tuple input) {
            String line = input.getStringByField("line");
            String[] words = line.split(",");

            for (String word:words){
                this.collector.emit(new Values(word));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    /**
     * 词频汇总Bolt
     */
    public static class CountBolt extends BaseRichBolt{

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        }

        Map<String,Integer> map = new HashMap<>();
        /**
         * 业务逻辑：
         * 1）获取每个单词
         * 2）对所有单词进行汇总
         * 3）输出
         */
        @Override
        public void execute(Tuple tuple) {
            //1）获取每个单词
            String word = tuple.getStringByField("word");
            Integer count = map.get(word);
            if (count==null){
                count=0;
            }
            //2）对所有单词进行汇总
            count++;
            map.put(word,count);
            //3）输出
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~");
            Set<Map.Entry<String, Integer>> entrySet = map.entrySet();
            for (Map.Entry<String, Integer> entry:entrySet){
                System.out.println(entry);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }
    }

    public static void main(String[] args) {
        // 通过TopologyBuilder根据Spout和Bolt构建Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout",new DataSourceSpout());
        builder.setBolt("SplitBolt",new SplitBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("CountBolt",new CountBolt()).shuffleGrouping("SplitBolt");

        // 创建本地集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalWordCountStormTopology",new Config(),builder.createTopology());
    }
}
