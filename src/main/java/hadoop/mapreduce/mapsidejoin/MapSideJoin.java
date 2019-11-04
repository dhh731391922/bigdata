package hadoop.mapreduce.mapsidejoin;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @program:bigdata
 * @package:hadoop.mapreduce.mapsidejoin
 * @filename:MapSideJoin.java
 * @create:2019.09.27.10.14
 * @author:Administrator
 * @descrption.
 */
public class MapSideJoin {
    static class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

        Map<String,String> pdInfoMap=new HashMap<String,String>();
        Text k=new Text();
        /*
        * map程序的执行流程
        * setup
        * while(){map}
        * cleanup
        * */

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //缓存的文件在当前目录
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("pdts.txt")));
            String line;
            while (StringUtils.isNotEmpty(line=br.readLine())){
                String[] fields = line.split("[,]");
                pdInfoMap.put(fields[0],fields[1]);
            }
            br.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            String pdName = pdInfoMap.get(fields[1]);
            k.set(line+"\t"+pdName);
            context.write(k,NullWritable.get());

        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(MapSideJoin.class);
        job.setMapperClass(MapSideJoinMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //指定需要缓存一个文件到所有的maptask运行节点的工作目录
//        job.addArchiveToClassPath();  //缓存jar包到task运行节点的classpath中
//        job.addFileToClassPath();     //缓存普通文件到task运行节点的classpath中
//        job.addCacheArchive();        //缓存压缩包文件到task运行节点的工作目录
//        job.addCacheFile(new URI(""));//缓存普通文件到 task运行节点的工作目录
        /*
        * 将产品文件缓存到task工作节点的工作目录中去
        * */
        job.addCacheFile(new URI(""));


        /*
        * 不需要reduce阶段，将reduce数量设为0
        * */
        job.setNumReduceTasks(0);
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }
}
