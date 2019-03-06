package com.ldz.bigdata;

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
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * @Author: Dazhou Li
 * @Description:使用storm实现累积求和
 * @CreateDate: 2019/2/7 0007 22:22
 */
public class LocalSumStormTopology {
    /**
     * Spout需要继承BaseRichSpout
     * 数据源需要产生数据并发射
     */
    public static class DataSourceSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;

        /**
         * 初始化方法，只会被调用一次
         *
         * @param conf      配置参数
         * @param context   上下文
         * @param collector 数据发射器,在nextTuple()中会用到
         */
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        int number = 0;

        /**
         * 会产生数据，在生产环境上肯定是从消息队列中获取数据
         * <p>
         * 这个方法是一个死循环，会一直不停的执行
         */
        @Override
        public void nextTuple() {
            ++number;
            /**
             * emit方法有两个参数：
             *  1） 数据
             *  2） 数据的唯一编号 msgId    如果是数据库，msgId就可以采用表中的主键
             */
            this.collector.emit(new Values(++number),number);
            System.out.println("Spout: " + number);

            // 防止数据产生太快
            Utils.sleep(1000);

        }

        /**
         * 声明输出字段,用于声明当前Spout/Bolt发送的tuple的名称，以便于下一个Bolt接收。
         * Fields()中的参数与nextTuple方法Values()中的参数个数一样多！
         *
         * @param declarer
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("num"));
        }

        @Override
        public void ack(Object msgId) {
            System.out.println(" ack invoked ..." + msgId);
        }

        @Override
        public void fail(Object msgId) {
            System.out.println(" fail invoked ..." + msgId);
        }
    }

    /**
     * 数据的累积求和Bolt：接收数据并处理
     */
    public static class SumBolt extends BaseRichBolt {

        private OutputCollector collector;

        /**
         * 初始化方法，会被执行一次
         *
         * @param stormConf
         * @param context
         * @param collector
         */
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector=collector;
        }

        int sum = 0;

        /**
         * 其实也是一个死循环，职责：获取Spout发送过来的数据
         *
         * @param input
         */
        @Override
        public void execute(Tuple input) {
            Integer num = input.getIntegerByField("num");
            sum += num;

            // 假设大于10的就是失败
            if (num>0 && num<=10){
                this.collector.ack(input); // 确认消息处理成功
            }else {
                this.collector.fail(input); // 确认消息处理失败
            }

            System.out.println("Bolt: sum = [" + sum + "]");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }
    }

    public static void main(String[] args) {
        // TopologyBuilder根据Spout和Bolt来构建出Topology
        // Storm中任何一个作业都是通过Topology的方式进行提交的
        // Topology中需要指定Spout和Bolt的执行顺序
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        builder.setBolt("SumBolt", new SumBolt()).shuffleGrouping("DataSourceSpout");

        // 创建一个本地Storm集群：本地模式运行，不需要搭建Storm集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalSumStormTopology", new Config(), builder.createTopology());
    }
}
