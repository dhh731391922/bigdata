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
import java.util.Iterator;

/**
 * @program:bigdata
 * @package:hadoop.mapreduce.grms
 * @filename:GoodsCooccurrenceList.java
 * @create:2019.10.10.14.53
 * @author:Administrator
 * @descrption.
 */
public class GoodsCooccurrenceList extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in=new Path(conf.get("in"));
        Path out=new Path(conf.get("out"));
        Job job = Job.getInstance(conf, "推荐系统:步骤二");
        job.setJarByClass(this.getClass());

        job.setMapperClass(GoodsCooccurrenceListMapper.class);
        job.setReducerClass(GoodsCooccurrenceListReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);



        TextInputFormat.addInputPath(job,in);
        TextOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GoodsCooccurrenceList(),args));
    }
    static class GoodsCooccurrenceListMapper extends Mapper<LongWritable, Text,Text,Text>{
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
    static class GoodsCooccurrenceListReduce extends Reducer<Text, Text,Text,Text>{
        Text k2=new Text();
        Text v2=new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            Text next = iterator.next();
            String[] split = next.toString().split("[,]");
            for (int i=0;i<split.length;i++){
                for (int j=0;j<split.length;j++){
                    k2.set(split[i]);
                    v2.set(split[j]);
                    context.write(k2,v2);
                }
            }
        }
    }
}
