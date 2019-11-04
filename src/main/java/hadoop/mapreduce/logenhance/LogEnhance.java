package hadoop.mapreduce.logenhance;

import hadoop.mapreduce.mapsidejoin.MapSideJoin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @program:bigdata
 * @package:hadoop.mapreduce.logenhance
 * @filename:LogEnhance.java
 * @create:2019.09.29.09.53
 * @author:Administrator
 * @descrption.
 */
public class LogEnhance {
    static class LogEnhanceMapper extends Mapper<LongWritable, Text,Text, NullWritable>{
        Map<String,String> ruleMap=new HashMap<>();

        //从数据库中加载规则信息到ruleMap中
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //JDBC获取数据
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Counter counter = context.getCounter("malformed", "malformedline");
            String line = value.toString();
            String[] fields = line.split("\t");

            try {
                String url = fields[26];
                String content_tag = ruleMap.get(url);
                //判断内容是否为空，为空只输出URL到带爬清单，有则输出到增强日志
                if (content_tag==null){
                    context.write(new Text(url+"\t"+"tocrawl"+"\n"),NullWritable.get());
                }else {
                    context.write(new Text(line+"\t"+content_tag+"\n"),NullWritable.get());
                }
            }catch (Exception e){
                counter.increment(1);
            }
        }

        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);
            job.setJarByClass(LogEnhance.class);
            job.setMapperClass(LogEnhanceMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            job.setOutputFormatClass(LogEnhanceOutputFormat.class);
            FileInputFormat.setInputPaths(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[1]));




            /*
             * 不需要reduce阶段，将reduce数量设为0
             * */
            job.setNumReduceTasks(0);
            boolean res = job.waitForCompletion(true);
            System.exit(res?0:1);
        }
    }
}
