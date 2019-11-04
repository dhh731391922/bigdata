package hadoop.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

/**
 * @program:bigdata
 * @package:hadoop.hdfs
 * @filename:HdfsStreamAccess.java
 * @create:2019.09.23.09.21
 * @author:Administrator
 * @descrption.用流的方式来操作hdfs上的文件，可以实现指定偏移量的数据
 */
public class HdfsStreamAccess {
    FileSystem fs=null;
    Configuration conf=null;
    @Before
    public void init() throws Exception {

        conf=new Configuration();
        fs=FileSystem.get(new URI("hdfs://192.168.186.5:9000"),conf,"hadoop");
    }
    /*
     * 通过流的方式上传 数据
     * */
    @Test
    public void testUpload() throws IOException {
        FSDataOutputStream ot = fs.create(new Path("/testStream.txt"));
        FileInputStream is = new FileInputStream("D:\\大数据\\大数据全套 (已分享)\\文档资料\\day07\\day07\\day06的问题总结.txt");
        IOUtils.copy(is,ot);
        //上传文件需要压缩，需要编码器、
        /*
        * 1.CompressionCodecFactory工厂模式自动获取，会以文件名尾作为依据
        * 2.在代码中手动new相应编码器
        * */
        String filePath="a.gz";
        CompressionCodecFactory factory=new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(new Path(filePath));
        CompressionOutputStream outputStream = codec.createOutputStream(ot);
        FileInputStream fis = new FileInputStream("D:\\大数据\\大数据全套 (已分享)\\文档资料\\day07\\day07\\day06的问题总结.txt");
        IOUtils.copy(fis,outputStream);
    }

    /*
    * 通过流的方式获取hdfs上的数据
    * */
    @Test
    public void testDownload() throws IOException {
        FSDataInputStream in = fs.open(new Path("/testStream.txt"));
        FileOutputStream out = new FileOutputStream("d:\\1.txt");
        IOUtils.copy(in,out);
    }
    /*
     * 通过流的方式获取指定直接大小hdfs上的数据
     * */
    @Test
    public void testRandomAccess() throws IOException {
        FSDataInputStream in = fs.open(new Path("/testStream.txt"));
        in.seek(12);
        FileOutputStream out = new FileOutputStream("d:\\1.txt.rang");
        IOUtils.copy(in,out);
    }
}
