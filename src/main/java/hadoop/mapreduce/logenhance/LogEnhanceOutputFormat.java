package hadoop.mapreduce.logenhance;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @program:bigdata
 * @package:hadoop.mapreduce.logenhance
 * @filename:LogEnhanceOutputFormat.java
 * @create:2019.09.29.10.13
 * @author:Administrator
 * @descrption.
 */
public class LogEnhanceOutputFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());
        Path enhancePath = new Path("hdfs://192.168.43.138:9000/enhancelog/log.data");
        Path tocrawlPath = new Path("hdfs://192.168.43.138:9000/enhancelog/url.dat");
        FSDataOutputStream enhanceOs = fs.create(enhancePath);
        FSDataOutputStream tocrawlOs = fs.create(tocrawlPath);
        return new  EnhanceRecordWriter(enhanceOs,tocrawlOs)  ;
    }

    /*
    * maptask后者reducetask在最终输出时，先调用OutputFormat的getRecordWriter方法拿到一个RecordWriter
    * 然后在调用RecordWriter的write方法将数据写出
    * */


    static class EnhanceRecordWriter extends RecordWriter<Text,NullWritable> {
        FSDataOutputStream enhanceOs=null;
        FSDataOutputStream tocrawlOs=null;

        public EnhanceRecordWriter(FSDataOutputStream enhanceOs, FSDataOutputStream tocrawlOs) {
            this.enhanceOs = enhanceOs;
            this.tocrawlOs = tocrawlOs;
        }

        @Override
        public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
            String result = text.toString();
            if (result.contains("tocrawl")){
                tocrawlOs.write(result.getBytes());
            }else {
                enhanceOs.write(result.getBytes());
            }

        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if (tocrawlOs!=null){
                tocrawlOs.close();
            }
            if (enhanceOs!=null){
                enhanceOs.close();
            }
        }
    }
}
