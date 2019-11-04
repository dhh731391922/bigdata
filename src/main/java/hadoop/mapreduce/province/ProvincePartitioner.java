package hadoop.mapreduce.province;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;


/**
 * @program:bigdata
 * @package:hadoop.mapreduce.province
 * @filename:ProvincePartitioner.java
 * @create:2019.09.25.09.27
 * @author:Administrator
 * @descrption.
 */
/*
* 泛型1,2是map的kv
* */
public class ProvincePartitioner extends Partitioner<Text,FlowBean> {
    public static HashMap<String,Integer> prId=new HashMap<String, Integer>();
    static {
        prId.put("136",0);
        prId.put("137",1);
        prId.put("138",2);
        prId.put("139",3);
    }

    public int getPartition(Text text, FlowBean flowBean, int i) {
        String prefix = text.toString().substring(0, 3);
        Integer provinceId = prId.get(prefix);
        return (provinceId==null)?4:provinceId;
    }
}
