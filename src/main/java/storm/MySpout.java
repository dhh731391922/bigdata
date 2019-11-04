package storm;


import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @program:bigdata
 * @package:storm
 * @filename:MySpout.java
 * @create:2019.10.13.10.03
 * @author:Administrator
 * @descrption.
 */
public class MySpout extends BaseRichSpout {
    SpoutOutputCollector spoutOutputCollector;

    //初始化方法
    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
    }
    //storm框架while(true)此方法
    @Override
    public void nextTuple() {
        spoutOutputCollector.emit(new Values("i love you ha ha y"));
    }
    //消息对应的角标
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("love"));
    }
}
