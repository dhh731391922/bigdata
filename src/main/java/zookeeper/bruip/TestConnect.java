package zookeeper.bruip;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * @program:bigdata
 * @package:zookeeper.bruip
 * @filename:TestConnect.java
 * @create:2019.10.15.16.57
 * @author:Administrator
 * @descrption.
 */
public class TestConnect {
    //zk对象的创建是异步执行的
    //在new ZooKeeper对象的过程中，主线程辉新开启一个子线程来连接zk的服务器，如果连接成功则会返回该对象
    public static void main(String[] args) throws IOException {
        ZooKeeper zk = new ZooKeeper("salve1:2181", 5000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("创建zk对象的线程名称"+Thread.currentThread().getName());
                System.out.println("观察到的事件"+watchedEvent);
            }
        });
        System.out.println(zk);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(zk);
    }
}
