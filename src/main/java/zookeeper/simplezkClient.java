package zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @program:bigdata
 * @package:zookeeper
 * @filename:simplezkClient.java
 * @create:2019.09.19.14.25
 * @author:Administrator
 * @descrption.简单的zookeeper客户程序
 *
 * 可以观察的操作：
 *      exists，getdata，getchilden
 *  可以触发观察的操作： (事件的类型：Watcher.Event.EventType.x)
 *      create delete setData
 *
 */
public class simplezkClient {
    private static final String connectString="192.168.186.5:2181,192.168.186.4:2181,192.168.186.3:2181";
    private static final int sessionTimout=2000;
    ZooKeeper zkClient=null;
    @Before
    public void init() throws IOException {
        zkClient=new ZooKeeper(connectString, sessionTimout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                //收到事件的处理函数

                System.out.println(watchedEvent.getType()+"..."+watchedEvent.getPath());
                try {
//                    循环注册监听器
                    zkClient.getChildren("/",true);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /*
     * 数据的增删改查
     * */
    //创建数据节点到zk中
    @Test
    public void testCreate() throws Exception {

        //参数1：要创建的节点路径
        //参数2：节点的数据
        //参数3：节点的权限，通常用ZooDefs.Ids.OPEN_ACL_UNSAFE
        //参数4：节点的类型
        String nodeCreated = zkClient.create("/ideal", "dhh".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(nodeCreated);
    }

    //判断znode是否存在
    @Test
    public void testExist() throws KeeperException, InterruptedException {
        //返回对象是节点除数据的值的封装
        Stat stat = zkClient.exists("/ideal", false);
        System.out.println(stat==null?"not exist":"exist");
    }

    //获取子节点
    @Test
    public void getChildren() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", true);
        for (String child:children){
            System.out.println(child);
        }
        //让程序一直运行
        Thread.sleep(Long.MAX_VALUE);

    }
    //获取znode的数据
    @Test
    public void getData() throws KeeperException, InterruptedException {
        byte[] data = zkClient.getData("/ideal", false, null);
        System.out.println(new String(data));
    }

    //删除znode的数据
    @Test
    public void deleteZode() throws KeeperException, InterruptedException {
        zkClient.delete("/ideal",-1);//-1代表全部删除
    }
    //修改znode的数据
    @Test
    public void setData() throws KeeperException, InterruptedException {
        zkClient.setData("/apps","dhh".getBytes(),-1);
        byte[] data = zkClient.getData("/apps", false, null);
        System.out.println(new String(data));
    }
}
