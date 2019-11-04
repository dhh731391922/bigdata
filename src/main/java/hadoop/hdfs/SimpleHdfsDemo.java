package hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;

/**
 * @program:bigdata
 * @package:hadoop.hdfs
 * @filename:SimpleHdfsDemo.java
 * @create:2019.09.22.20.19
 * @author:Administrator
 * @descrption.
 */
public class SimpleHdfsDemo {
    FileSystem fs=null;
    Configuration conf=null;
    @Before
    public void init() throws Exception {

        conf=new Configuration();
//        conf.set("fs.defaultFS","hdfs://master:9000");
//        这种方式需要在运行时加参数指定用户-DHADOOP_USER_NAME=hadoop
        fs=FileSystem.get(new URI("hdfs://192.168.186.5:9000"),conf,"hadoop");
    }
    /*
    * 上传文件
    * */
    @Test
    public void testUpload() throws Exception {
        fs.copyFromLocalFile(
                new Path("D:\\大数据\\大数据全套 (已分享)\\文档资料\\day06\\day06\\hadoop2.4.1集群搭建.txt"),
                new Path("/test1.txt"));
        fs.close();
    }
    /*
    * 下载文件
    * */
    @Test
    public void testDownload() throws IOException {
        fs.copyToLocalFile(new Path("/test1.txt"),new Path("d:\\"));
        fs.close();
    }
    /*
    打印参数
    * */
    @Test
    public void testConf(){
        Iterator<Map.Entry<String, String>> it =   conf.iterator();
        while (it.hasNext()){
            Map.Entry<String, String> next = it.next();
            System.out.println(next.getKey()+":"+next.getValue());
        }
    }
    @Test
    public void testMkdir() throws IOException {
        boolean mkdirs = fs.mkdirs(new Path("/testMkdir"));
        System.out.println(mkdirs);
        fs.close();
    }
    @Test
    public void testDelete() throws IOException {
        //第二个参数表示是否递归删除
        boolean delete = fs.delete(new Path("/testMkdir"), true);
        System.out.println(delete);
    }
    /*
    * 递归查看数据
    * 数据量大时返回的是迭代器，因为迭代器不用存储数据
    * */
    @Test
    public void testLs() throws IOException {
        RemoteIterator<LocatedFileStatus> ls = fs.listFiles(new Path("/"), true);
        while (ls.hasNext()){
            LocatedFileStatus next = ls.next();
            System.out.println("blocksize"+next.getBlockSize());
            System.out.println("owner"+next.getOwner());
            System.out.println("replication"+next.getReplication());
            System.out.println("Permission"+next.getPermission());
            System.out.println("name"+next.getPath().getName());
            System.out.println("------------");
            /*
            * 文件的块位置信息
            * */
            BlockLocation[] bl = next.getBlockLocations();
            for (BlockLocation b:bl ){
                String[] hosts = b.getHosts();
                b.getOffset();
            }
        }
    }
    /*
    * 不会递归返回数据
    * */
    @Test
    public void testLs2() throws IOException {
        FileStatus[] f = fs.listStatus(new Path("/"));
        for (FileStatus file:f){
            System.out.println(file.getPath().getName());
        }
    }
}
