package hadoop.mapreduce.fans;

import hadoop.mapreduce.inverindex.InverIndexTwo;
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
 * @package:hadoop.mapreduce.fans
 * @filename:SharedFriends.java
 * @create:2019.09.28.10.03
 * @author:Administrator
 * @descrption.
 */
public class SharedFriendsStepOne {
    static class SharedFriendsStepOneMapper extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] person_friend = value.toString().split(":");
            for (String friend:person_friend[1].split(",")){
                //一个好友有哪些人
                context.write(new Text(friend),new Text(person_friend[0]));
            }
        }
    }
    static class SharedFriendsStepOneReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            for (Text person:values){
                sb.append(person).append(",");
            }
            context.write(key,new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //指定本程序的jar包所在的本地路径
        job.setJarByClass(InverIndexTwo.class);



        //指定本业务job要使用的mapper/reduce业务类
        job.setMapperClass(SharedFriendsStepOneMapper.class);
        job.setReducerClass(SharedFriendsStepOneReducer.class);



        //指定最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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
