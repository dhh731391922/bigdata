package hadoop.mapreduce.inverindex;

import hadoop.mapreduce.join.InfoBean;
import hadoop.mapreduce.join.RJoin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @program:bigdata
 * @package:hadoop.mapreduce.inverindex
 * @filename:InverIndexStepOne.java
 * @create:2019.09.28.09.07
 * @author:Administrator
 * @descrption.
 */
public class InverIndexStepOne {
    static class InverIndexStepOneMapper extends Mapper<LongWritable, Text,Text, IntWritable>{
        Text k=new Text();
        IntWritable v=new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String fileName = inputSplit.getPath().getName();
            for (String word:words  ){
                k.set(word+"--"+fileName);
                context.write(k,v);
            }
        }
    }
    static class InverIndexStepOneReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count =0;
            for (IntWritable value:values){
                count+=value.get();
            }
            context.write(key,new IntWritable(count));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //指定本程序的jar包所在的本地路径
        job.setJarByClass(InverIndexStepOne.class);



        //指定本业务job要使用的mapper/reduce业务类
        job.setMapperClass(InverIndexStepOneMapper.class);
        job.setReducerClass(InverIndexStepOneReducer.class);



        //指定最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

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
