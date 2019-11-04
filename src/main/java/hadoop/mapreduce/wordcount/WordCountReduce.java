package hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @program:bigdata
 * @package:hadoop.mapreduce.wordcount
 * @filename:WordCountReduce.java
 * @create:2019.09.24.09.47
 * @author:Administrator
 * @descrption.
 */

/*
* 1,2:对应map的输出
* 3,4：自定义reduce程序的输出结果
* */
public class WordCountReduce extends Reducer<Text, IntWritable,Text,IntWritable> {
    /*
    * <a,1><a,1><a,1><a,1><a,1>
      <b,1><b,1><b,1><b,1><b,1>
      * key是一组单词的key
    * */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count =0;
//        可以用迭代器
//        Iterator<IntWritable> iterator = values.iterator();
//        也可以用for
        for (IntWritable value:values){
            count+=value.get();
        }
        context.write(new Text(key),new IntWritable(count));
    }
}
