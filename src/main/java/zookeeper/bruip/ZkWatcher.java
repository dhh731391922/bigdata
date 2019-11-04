package zookeeper.bruip;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @program:bigdata
 * @package:zookeeper.bruip
 * @filename:ZkWatcher.java
 * @create:2019.10.15.17.28
 * @author:Administrator
 * @descrption.
 */
public class ZkWatcher implements Watcher {
    private String connStr;
    private CountDownLatch cdl;
    private ZooKeeper zk;
    public ZkWatcher(String connStr){
        this.connStr=connStr;
        this.cdl=new CountDownLatch(1);
    }
    public void connect(){
        try {
            this.zk=new ZooKeeper(this.connStr,5000,this);
            this.cdl.await();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getState()==Event.KeeperState.SyncConnected ){
            this.cdl.countDown();
        }
    }
    public ZooKeeper getZk(){
        return this.zk;
    }
}
