package hadoop.mapreduce.wordcount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program:bigdata
 * @package:hadoop.mapreduce.wordcount
 * @filename:WordCountMapper.java
 * @create:2019.09.24.09.08
 * @author:Administrator
 * @descrption.
 */

/*
* 1:默认情况下，是mr框架所读到的一行文本的起始偏移量，Long
*   但是在hadoop中有更精简的序列化接口，所有不能直接用Long，而用LongWritable
* 2:默认情况下是mr框架所读到的一行文本内容，String ,同上Text
* 3:是用户自定义逻辑处理完成之后输出数据中的key，在此处是单词，String,同上Text
* 4：是用户自定义逻辑处理完成之后输出数据中的value，在此处是单词次数，Integer,同上IntWritable
* */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    /*
    * map阶段的业务逻辑就写在自定义的map方法中
    * maptask会对每一行输入数据调用一次我们自定义的map（）方法
    * */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //将maptask传给我们的文本内容转换成String
        String line = value.toString();
        //根据空格切割
        String[] words = line.split(" ");
        //将单词输出<单词,1>
        for (String word: words){
            //将单词作为key，次数作为value，以便后续的数据分发，以便于相同的单词会到相同的reducetask
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
