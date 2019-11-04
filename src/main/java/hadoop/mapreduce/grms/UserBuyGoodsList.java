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

/**
 * @program:bigdata
 * @package:hadoop.mapreduce.grms
 * @filename:UserBuyGoodsList.java
 * @create:2019.10.10.14.19
 * @author:Administrator
 * @descrption.
 */
public class UserBuyGoodsList extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in=new Path(conf.get("in"));
        Path out=new Path(conf.get("out"));
        Job job = Job.getInstance(conf, "推荐系统:步骤一");
        job.setJarByClass(this.getClass());

        job.setMapperClass(UserBuyGoodsListMapper.class);
        job.setReducerClass(UserBuyGoodsListReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        TextInputFormat.addInputPath(job,in);
        TextOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new UserBuyGoodsList(),args));
    }
    static class UserBuyGoodsListMapper extends Mapper<LongWritable, Text,Text,Text>{
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
    static class UserBuyGoodsListReduce extends Reducer<Text,Text,Text,Text>{
        Text v2=new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer buffer = new StringBuffer();
            for (Text v:values){
                buffer.append(v.toString()).append(",");
            }
            v2.set(buffer.toString().substring(0,buffer.length()-1));
            context.write(key,v2);
        }
    }
}
