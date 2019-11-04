package hadoop.mapreduce.join;

import hadoop.mapreduce.province.FlowBean;
import hadoop.mapreduce.province.FlowCount;
import hadoop.mapreduce.province.ProvincePartitioner;
import org.apache.commons.beanutils.BeanUtils;
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
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * @program:bigdata
 * @package:hadoop.mapreduce.join
 * @filename:RJoin.java
 * @create:2019.09.26.10.18
 * @author:Administrator
 * @descrption.
 */
public class RJoin {
    static class RJoinMapper extends Mapper<LongWritable, Text,Text,InfoBean>{
        InfoBean bean=new InfoBean();
        Text k=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();


            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            //拿到文件名
            String name = inputSplit.getPath().getName();
            String pid="";
            //通过文件名判断是哪种数据
            if (name.endsWith("order")){
                String[] fields = line.split(",");
                pid=fields[2];
                bean.set(Integer.parseInt(fields[0]),fields[1],pid,Integer.parseInt(fields[3]),
                        "",0,0,"0");


            }else {
                String[] fields = line.split(",");
                pid=fields[0];
                bean.set(0,"",pid,0,fields[1],Integer.parseInt(fields[2]),Float.parseFloat(fields[3]),
                        "1");

            }
            k.set(pid);
            context.write(k,bean);
        }
    }
    static class RJoinReducer extends Reducer<Text,InfoBean,InfoBean, NullWritable>{

        @Override
        protected void reduce(Text key, Iterable<InfoBean> values, Context context) throws IOException, InterruptedException {
            InfoBean pdBean = new InfoBean();
            ArrayList<InfoBean> orderBeans = new ArrayList<>();
            for (InfoBean bean:values){
                if ("1".equals(bean.getFlag())){
                    try {
                        BeanUtils.copyProperties(pdBean,bean);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }

                }else {
                    InfoBean odBean = new InfoBean();
                    try {
                        BeanUtils.copyProperties(odBean,bean);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                    orderBeans.add(odBean);

                }
            }
            for (InfoBean bean :orderBeans){
                bean.setPname(pdBean.getPname());
                bean.setCategory_id(pdBean.getCategory_id());
                bean.setPrice(pdBean.getPrice());
                context.write(bean,NullWritable.get());
            }
        }

        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);
            //指定本程序的jar包所在的本地路径
            job.setJarByClass(RJoin.class);



            //指定本业务job要使用的mapper/reduce业务类
            job.setMapperClass(RJoinMapper.class);
            job.setReducerClass(RJoinReducer.class);

            //指定mapper输出的kv类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(InfoBean.class);

            //指定最终的输出类型
            job.setOutputKeyClass(InfoBean.class);
            job.setOutputValueClass(NullWritable.class);

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
}
