package storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * @program:bigdata
 * @package:storm
 * @filename:WordCountTopolog.java
 * @create:2019.10.13.09.48
 * @author:Administrator
 * @descrption.
 */
public class WordCountTopolog {
    public static void main(String[] args) throws Exception {
        //1.准备一个TopologyBuilder
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("mySpout",new MySpout(),1);
        topologyBuilder.setBolt("myBolt1",new MySplitBolt(),10).shuffleGrouping("mySpout");
        topologyBuilder.setBolt("myBolt2",new MycountBolt(),2).fieldsGrouping("myBolt1",new Fields("word"));
        //2.创建一个configuration对象，用来指定当前topology需要的worker的数量
        Config config = new Config();
        config.setNumWorkers(2);
        //3.提交任务（两种模式：本地模式和集群模式）
        StormSubmitter.submitTopology("mywordcount",config,topologyBuilder.createTopology());
//        LocalCluster localCluster = new LocalCluster();
//        localCluster.submitTopology("mywordcount",config,topologyBuilder.createTopology());
    }
}
