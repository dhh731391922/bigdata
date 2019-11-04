package hadoop.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @program:bigdata
 * @package:hadoop.mapreduce.wordcount
 * @filename:WordCountDriver.java
 * @create:2019.09.24.10.09
 * @author:Administrator
 * @descrption.相当于一个yarn集群的客户端
 * 需要再此封装我们的mr程序的相关参数，指定jar包
 * 最后提交给yarn
 */
public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

    //        //提交到本地模拟运行，不做任何配置在本地运行默认就是在本地跑
    //        conf.set("mapreduce.framework.name","local");
//        本地模式运行mr程序时，输入输出的数据可以在本地，也可以在hdfs上
    //        conf.set("fs.defaultFS","file:///");




        //指定本程序的jar包所在的本地路径,集群上运行
        job.setJarByClass(WordCountDriver.class);

        //指定本业务job要使用的mapper/reduce业务类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);

        //指定mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        job.setCombinerClass(WordcountCombiner.class);
        job.setCombinerClass(WordCountReduce.class);

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
