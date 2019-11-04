package storm;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * @program:bigdata
 * @package:storm
 * @filename:MycountBolt.java
 * @create:2019.10.13.10.10
 * @author:Administrator
 * @descrption.
 */
public class MycountBolt extends BaseRichBolt {
    OutputCollector outputCollector;
    Map<String,Integer> map=new HashMap<>();

    //初始化方法
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
    }
    //storm框架while(true)此方法
    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        Integer num = tuple.getInteger(1);
        map.put(word,map.containsKey(word)?map.get(word)+1:1);
        System.out.println("count"+map);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //不输出
    }
}
