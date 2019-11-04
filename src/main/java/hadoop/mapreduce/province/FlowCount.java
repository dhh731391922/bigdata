package hadoop.mapreduce.province;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @program:bigdata
 * @package:hadoop.mapreduce.flowsum
 * @filename:FlowCountMapper.java
 * @create:2019.09.24.16.25
 * @author:Administrator
 * @descrption.
 */
public class FlowCount {

    static class FlowCountMapper extends Mapper<LongWritable, Text,Text, FlowBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //将一行内容转成string
            String line = value.toString();
            //切分自动
            String[] fields = line.split("\t");
            //取出手机号
            String phoneNbr=fields[1];
            //取出上行流量
            long upFlow=Long.parseLong(fields[fields.length-3]);
            //取出下行流量
            long dFlow=Long.parseLong(fields[fields.length-2]);
            context.write(new Text(phoneNbr),new FlowBean(upFlow,dFlow));
        }
    }

    static class FlowCountReduce extends Reducer<Text, FlowBean,Text, FlowBean> {
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long sum_upFlow=0;
            long sum_dFlow=0;

            for (FlowBean bean:values){
                sum_upFlow+=bean.getUpFlow();
                sum_dFlow+=bean.getdFlow();
            }
            context.write(new Text(key),new FlowBean(sum_upFlow,sum_dFlow));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //指定本程序的jar包所在的本地路径
        job.setJarByClass(FlowCount.class);

        //指定我们自定义的数据分区
        job.setPartitionerClass(ProvincePartitioner.class);
        //同时指定相应数据分区数量的reducetask
        job.setNumReduceTasks(5);

        //指定本业务job要使用的mapper/reduce业务类
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReduce.class);

        //指定mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //指定最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //指定job的输入原始文件的所在目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //将job中配置的相关参数以及job以及job所用的java类所在的jar包，提交给yarn去运行、
//        job.submit();//看不到结果
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }

}
