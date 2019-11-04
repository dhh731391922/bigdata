package zookeeper.application.topandbottomline;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @program:bigdata
 * @package:zookeeper.application.topandbottomline
 * @filename:DistributedServer.java
 * @create:2019.09.19.16.46
 * @author:Administrator
 * @descrption.
 */
public class DistributedServer {
    private ZooKeeper zk=null;
    private static final String connectString="192.168.186.5:2181,192.168.186.4:2181,192.168.186.3:2181";
    private static final int sessionTimout=2000;
    private static final String parentNode="/services";
    public void getConnect() throws IOException {
        zk=new ZooKeeper(connectString, sessionTimout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getType()+"..."+watchedEvent.getPath());
                try {
                    zk.getChildren("/",true);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public void registerServer(String hostname) throws KeeperException, InterruptedException {
        String s = zk.create(parentNode + "/service", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname+"is online.."+s);
    }
    //业务功能
    public void handleBussiness(String hostname) throws InterruptedException {
        System.out.println(hostname+"start working..");
        Thread.sleep(Long.MAX_VALUE);
    }
    public static void main(String[] args) throws Exception {
        //获取jk连接
        DistributedServer server = new DistributedServer();
        server.getConnect();
        //利用zk连接注册服务器信息
        server.registerServer(args[0]);
        //启动业务逻辑
        server.handleBussiness(args[0]);
    }
}
