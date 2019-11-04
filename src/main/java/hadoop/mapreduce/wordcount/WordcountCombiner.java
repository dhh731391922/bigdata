package hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program:bigdata
 * @package:hadoop.mapreduce.wordcount
 * @filename:WordcountCombiner.java
 * @create:2019.09.25.21.20
 * @author:Administrator
 * @descrption.
 */
public class WordcountCombiner extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count=0;
        for (IntWritable v:values){
            count+=v.get();
        }
        context.write(key,new IntWritable(count));
    }
}
