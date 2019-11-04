package hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 * @package:hadoop.mapreduce
 * @filename:PatentCites.java
 * @create:2019.09.26.16.36
 * @author:Administrator
 * @descrption.
 */
public class PatentCites extends Configured implements Tool {
    static class PatentCitesMapper extends Mapper<LongWritable, Text,Text, Text>{
        private Text k2=new Text();
        private Text v2=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("[,]");
            this.k2.set(strs[0]);
            this.v2.set(strs[1]);
            context.write(this.k2,this.v2);
        }
    }
    static class PatentCitesReducer extends Reducer<Text,Text,Text,Text>{
        Text v3=new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb=new StringBuilder();
            values.forEach(v2->{
                sb.append(v2.toString()).append(",");
            });
            v3.set(sb.toString().substring(0,sb.length()));
            System.out.println("reduce"+key+v3);
            context.write(key,v3);
        }
    }

    //3.作业配置，作业都写在run方法中，run方法会在客户端执行，MapReduce执行在集群中
    //在run方法的打印语句会出现在控制台上，MapReduce的打印语句不会，而是打印在日志文件中
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in=new Path(conf.get("in"));
        Path out=new Path(conf.get("out"));
        Job job = Job.getInstance(conf, "专利引用");
        job.setJarByClass(this.getClass());

        //map
        job.setMapperClass(PatentCitesMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //配置map任务读取任务的方式
        job.setInputFormatClass(TextInputFormat.class);
        //配置文件的输入路径
        TextInputFormat.addInputPath(job,in);

        //reduce
        job.setReducerClass(PatentCitesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        System.out.println("任务配置已经完成");


        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new PatentCites(),args));
    }
}
