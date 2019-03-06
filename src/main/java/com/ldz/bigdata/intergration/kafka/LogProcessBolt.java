package com.ldz.bigdata.intergration.kafka;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @Author: Dazhou Li
 * @Description:接收kafka的数据进行处理
 * @CreateDate: 2019/2/18 0018 19:20
 */
public class LogProcessBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            byte[] binaryByField = tuple.getBinaryByField("bytes");//必须这样写
            String value = new String(binaryByField);

            //解析日志信息
            String[] splits = value.split(" ");
            String phone = splits[0];
            String[] temp = splits[1].split(",");
            String longitude = temp[0];
            String latitude = temp[1];
            long time = DateUtils.getInstance().getTime(splits[2]+" "+splits[3]);

            System.out.println(phone + "," + longitude + "," + latitude + "," + time);

            collector.emit(new Values(time,longitude,latitude));

            this.collector.ack(tuple);
        } catch (Exception e) {
            this.collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time", "longitude", "latitude"));
    }
}
