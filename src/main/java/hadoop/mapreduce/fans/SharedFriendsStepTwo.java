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
import java.util.Arrays;

/**
 * @program:bigdata
 * @package:hadoop.mapreduce.fans
 * @filename:SharedFriendsStepTwo.java
 * @create:2019.09.28.10.24
 * @author:Administrator
 * @descrption.
 */
public class SharedFriendsStepTwo {
    static class SharedFriendsStepTwoMapper extends Mapper<LongWritable, Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] friend_persons = value.toString().split("\t");
            String friend = friend_persons[0];
            String[] persons = friend_persons[1].split(",");
            Arrays.sort(persons);
            for (int i=0;i<persons.length-2;i++){
                for (int j=i+1;j<persons.length-1;j++){
                    context.write(new Text(persons[i]+"-"+persons[j]),new Text(friend));
                }
            }

        }
    }
    static class SharedFriendsStepTwoReducer extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            for (Text person:values){
                sb.append(person).append(" ");
            }
            context.write(key,new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //指定本程序的jar包所在的本地路径
        job.setJarByClass(SharedFriendsStepTwo.class);



        //指定本业务job要使用的mapper/reduce业务类
        job.setMapperClass(SharedFriendsStepTwoMapper.class);
        job.setReducerClass(SharedFriendsStepTwoReducer.class);



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
