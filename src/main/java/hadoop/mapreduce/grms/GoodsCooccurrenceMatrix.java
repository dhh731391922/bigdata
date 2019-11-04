package hadoop.mapreduce.grms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * @program:bigdata
 * @package:hadoop.mapreduce.grms
 * @filename:GoodsCooccurrenceMatrix.java
 * @create:2019.10.10.15.15
 * @author:Administrator
 * @descrption.
 */
public class GoodsCooccurrenceMatrix extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in=new Path(conf.get("in"));
        Path out=new Path(conf.get("out"));
        Job job = Job.getInstance(conf, "推荐系统:步骤三");
        job.setJarByClass(this.getClass());

        job.setMapperClass(GoodsCooccurrenceMatrixMapper.class);
        job.setReducerClass(GoodsCooccurrenceMatrixReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        TextInputFormat.addInputPath(job,in);
        TextOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GoodsCooccurrenceMatrix(),args));
    }
    static class GoodsCooccurrenceMatrixMapper extends Mapper<LongWritable, Text,Text,Text>{
        Text k1=new Text();
        Text v1=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("[\t]");
            k1.set(fields[0]);
            v1.set(fields[1]);
            context.write(k1,v1);
        }
    }
    static class GoodsCooccurrenceMatrixReduce extends Reducer<Text,Text,Text,Text>{
        Text v2=new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            HashMap<Text, Integer> map = new HashMap<>();
            StringBuffer buffer = new StringBuffer();
            while (iterator.hasNext()){
                Text next = iterator.next();
                map.put(next,map.containsKey(next)?map.get(next)+1:1);
            }
            Set<Text> keys = map.keySet();
            for (Text t:keys){
                buffer.append(t.toString()).append(":").append(map.get(t)).append(",");
            }
            v2.set(buffer.toString().substring(0,buffer.length()));
            context.write(key,v2);
        }
    }
}
