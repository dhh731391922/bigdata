package hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @program:bigdata
 * @package:hbase
 * @filename:HbaseTest.java
 * @create:2019.10.09.21.42
 * @author:Administrator
 * @descrption.
 */
public class HbaseTest {
    static Configuration config=null;
    private Connection connection=null;
    private Table table=null;
    @Before
    public void init() throws IOException {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "salve1,salve2,salve3");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.master", "master:60000");
        connection=ConnectionFactory.createConnection(config);
        table = connection.getTable(TableName.valueOf("user"));
    }

    //创建表
    @Test
    public void createTable() throws IOException {
        //创建表的管理类
        HBaseAdmin admin = new HBaseAdmin(config);
        //创建表的描述类
        TableName test3 = TableName.valueOf("test3");
        HTableDescriptor desc = new HTableDescriptor(test3);
        //创建列族的描述信息
        HColumnDescriptor family = new HColumnDescriptor("info");
        HColumnDescriptor family2 = new HColumnDescriptor("info2");
        //将列族添加到表中
        desc.addFamily(family);
        desc.addFamily(family2);
        //创建表
        admin.createTable(desc);

    }

    //删除表
    @Test
    public void deleteTable() throws IOException {
        HBaseAdmin admin = new HBaseAdmin(config);
        admin.disableTable("test3");
        admin.deleteTable("test3");
        admin.close();
    }
    @Test
    //想Hbase中增加数据
    public void insertData() throws IOException {
        //单挑插入
        Put put = new Put(Bytes.toBytes("wangsenfeng_1234"));
        put.add(Bytes.toBytes("info1"),Bytes.toBytes("name"),Bytes.toBytes("zhangsan"));
        table.put(put);
        //多条插入
        ArrayList<Put> puts = new ArrayList<>();
        Put put1 = put.add(Bytes.toBytes("info1"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
        Put put2 = put.add(Bytes.toBytes("info1"), Bytes.toBytes("age"), Bytes.toBytes("18"));
        puts.add(put1);
        puts.add(put2);
        table.put(puts);

    }

    //没有更新的概念，只有覆盖
    @Test
    public void uodateData() throws Exception{
        Put put = new Put(Bytes.toBytes("1234"));
        put.add(Bytes.toBytes("info"), Bytes.toBytes("namessss"), Bytes.toBytes("lisi1234"));
        put.add(Bytes.toBytes("info"), Bytes.toBytes("password"), Bytes.toBytes(1234));
        //插入数据
        table.put(put);
        //提交
    }

    @Test
    //删除数据
    public void deleteDate() throws IOException {
        Delete delete = new Delete(Bytes.toBytes("wangsenfeng_1234"));
        delete.addFamily(Bytes.toBytes("info1"));//删除一个列族
        delete.addColumn(Bytes.toBytes("info1"),Bytes.toBytes("name"));//删除指定的类名
        table.delete(delete);
    }

    //查数据

    //单条查询
    @Test
    public void queryData() throws IOException {
        Get get = new Get(Bytes.toBytes("wangsenfeng_1234"));
        Result result = table.get(get);
        byte[] value = result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("name"));
        System.out.println(Bytes.toString(value));
    }
    //全局扫描
    public void scanData() throws IOException {
        Scan scan = new Scan();
        //scan.addFamily(Bytes.toBytes("info"));
        //scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password"));
        scan.setStartRow(Bytes.toBytes("wangsf_0"));
        scan.setStopRow(Bytes.toBytes("wangwu"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("password"))));
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
            //System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("password"))));
            //System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("name"))));
        }

    }

    //全表扫描的过滤器
	    //列值过滤器
    @Test
    public void scanDataByFilter1() throws Exception {
        // 创建全表扫描的scan
        Scan scan = new Scan();

        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info1"),
                Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes("zhangsan"));
        scan.setFilter(filter);// 设置过滤器

        ResultScanner scanner = table.getScanner(scan);

        // 打印结果集

        for (Result result : scanner) {
            System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("password"))));
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
            //System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("password"))));
            //System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("name"))));
        }

    }
            //rowkey过滤器
    @Test
    public void scanDataByFilter2() throws Exception {
        Scan scan = new Scan();
        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator("^1234"));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner){
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("password"))));
        }
    }

    //匹配列名前缀
    @Test
    public void scanDataByFilter3() throws Exception {
        Scan scan = new Scan();
        //匹配rowkey以wangsenfeng开头的
        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("na"));
        // 设置过滤器
        scan.setFilter(filter);
        // 打印结果集
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println("rowkey：" + Bytes.toString(result.getRow()));
            System.out.println("info:name："
                    + Bytes.toString(result.getValue(Bytes.toBytes("info"),
                    Bytes.toBytes("name"))));
            // 判断取出来的值是否为空
            if (result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")) != null) {
                System.out.println("info:age："
                        + Bytes.toInt(result.getValue(Bytes.toBytes("info"),
                        Bytes.toBytes("age"))));
            }
            // 判断取出来的值是否为空
            if (result.getValue(Bytes.toBytes("info"), Bytes.toBytes("sex")) != null) {
                System.out.println("infi:sex："
                        + Bytes.toInt(result.getValue(Bytes.toBytes("info"),
                        Bytes.toBytes("sex"))));
            }
            // 判断取出来的值是否为空
            if (result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("name")) != null) {
                System.out
                        .println("info2:name："
                                + Bytes.toString(result.getValue(
                                Bytes.toBytes("info2"),
                                Bytes.toBytes("name"))));
            }
            // 判断取出来的值是否为空
            if (result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("age")) != null) {
                System.out.println("info2:age："
                        + Bytes.toInt(result.getValue(Bytes.toBytes("info2"),
                        Bytes.toBytes("age"))));
            }
            // 判断取出来的值是否为空
            if (result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("sex")) != null) {
                System.out.println("info2:sex："
                        + Bytes.toInt(result.getValue(Bytes.toBytes("info2"),
                        Bytes.toBytes("sex"))));
            }
        }
    }

    //过滤器集合
    @Test
    public void scanDataByFilter4() throws Exception {
        // 创建全表扫描的scan
        Scan scan = new Scan();
        //过滤器集合：MUST_PASS_ALL（and）,MUST_PASS_ONE(or)
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);//[MUST_PASS_ONE or逻辑]MUST_PASS_ALL and逻辑
        //匹配rowkey以wangsenfeng开头的
        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^wangsenfeng"));
        //匹配name的值等于wangsenfeng
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("info"),
                Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes("zhangsan"));
        filterList.addFilter(filter);
        filterList.addFilter(filter2);
        // 设置过滤器
        scan.setFilter(filterList);
        // 打印结果集
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println("rowkey：" + Bytes.toString(result.getRow()));
            System.out.println("info:name："
                    + Bytes.toString(result.getValue(Bytes.toBytes("info"),
                    Bytes.toBytes("name"))));
            // 判断取出来的值是否为空
            if (result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")) != null) {
                System.out.println("info:age："
                        + Bytes.toInt(result.getValue(Bytes.toBytes("info"),
                        Bytes.toBytes("age"))));
            }
            // 判断取出来的值是否为空
            if (result.getValue(Bytes.toBytes("info"), Bytes.toBytes("sex")) != null) {
                System.out.println("infi:sex："
                        + Bytes.toInt(result.getValue(Bytes.toBytes("info"),
                        Bytes.toBytes("sex"))));
            }
            // 判断取出来的值是否为空
            if (result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("name")) != null) {
                System.out
                        .println("info2:name："
                                + Bytes.toString(result.getValue(
                                Bytes.toBytes("info2"),
                                Bytes.toBytes("name"))));
            }
            // 判断取出来的值是否为空
            if (result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("age")) != null) {
                System.out.println("info2:age："
                        + Bytes.toInt(result.getValue(Bytes.toBytes("info2"),
                        Bytes.toBytes("age"))));
            }
            // 判断取出来的值是否为空
            if (result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("sex")) != null) {
                System.out.println("info2:sex："
                        + Bytes.toInt(result.getValue(Bytes.toBytes("info2"),
                        Bytes.toBytes("sex"))));
            }
        }
    }
    @After
    public void close() throws IOException {
        table.close();
        connection.close();
    }

}
