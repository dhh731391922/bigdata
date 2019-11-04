package hadoop.mapreduce.grms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @program:bigdata
 * @package:hadoop.mapreduce.grms
 * @filename:MultiplyGoodsMatrixAndUserVector.java
 * @create:2019.10.10.17.04
 * @author:Administrator
 * @descrption.
 */
public class MultiplyGoodsMatrixAndUserVector extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in=new Path(conf.get("in"));
        Path out=new Path(conf.get("out"));
        Job job = Job.getInstance(conf, "推荐系统:步骤四");
        job.setJarByClass(this.getClass());

        job.setMapperClass(UserBuyGoodsVector.UserBuyGoodsVectorMapper.class);
        job.setReducerClass(UserBuyGoodsVector.UserBuyGoodsVectorReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        TextInputFormat.addInputPath(job,in);
        TextOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MultiplyGoodsMatrixAndUserVector(),args));
    }
    static class MultiplyGoodsMatrixAndUserVectorMapper extends Mapper<LongWritable, Text,Text,Text>{
        Text k1=new Text();
        Text v1=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("[\t]");
            k1.set(fields[0]);
            if (fields[1].startsWith("1")){
                v1.set(fields[1]+"y");
            }else {
                v1.set(fields[1]+"s");
            }
            context.write(k1,v1);
        }
    }
}
