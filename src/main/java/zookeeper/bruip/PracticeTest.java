package zookeeper.bruip;

import org.apache.storm.zookeeper.Zookeeper;
import org.apache.zookeeper.ZooKeeper;


/**
 * @program:bigdata
 * @package:zookeeper.bruip
 * @filename:PracticeTest.java
 * @create:2019.10.16.16.16
 * @author:Administrator
 * @descrption.递归删除
 */
public class PracticeTest {
    private ZooKeeper zk;
    public PracticeTest(){
        ZkWatcher zw = new ZkWatcher("zk01:2181,zk02:2181,zk03:2181");
        zw.connect();
        this.zk=zw.getZk();
    }

}
