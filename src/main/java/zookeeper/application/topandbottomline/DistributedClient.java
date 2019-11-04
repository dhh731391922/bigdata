package zookeeper.application.topandbottomline;

        import org.apache.zookeeper.KeeperException;
        import org.apache.zookeeper.WatchedEvent;
        import org.apache.zookeeper.Watcher;
        import org.apache.zookeeper.ZooKeeper;

        import java.io.IOException;
        import java.util.ArrayList;
        import java.util.List;

/**
 * @program:bigdata
 * @package:zookeeper.application.topandbottomline
 * @filename:DistributedClient.java
 * @create:2019.09.19.18.48
 * @author:Administrator
 * @descrption.
 */
public class DistributedClient {
    private ZooKeeper zk=null;
    private static final String connectString="192.168.186.5:2181,192.168.186.4:2181,192.168.186.3:2181";
    private static final int sessionTimout=2000;
    private static final String parentNode="/services";
    //加volatile
    private volatile List<String> serviceList;
    public void getConnect() throws IOException {
        zk=new ZooKeeper(connectString, sessionTimout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getType()+"..."+watchedEvent.getPath());
                try {
                    getServiceList();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }


    public void getServiceList() throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren(parentNode, true);
        List<String> services =new ArrayList<String>();
        for (String child:services){
            byte[] data = zk.getData(parentNode + "/" + child, false, null);
            services.add(new String(data));
        }
        serviceList=services;
        System.out.println(serviceList);
    }

    //业务功能
    public void handleBussiness() throws InterruptedException {
        System.out.println("client start working..");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {
        //获取zk连接
        DistributedClient client = new DistributedClient();
        client.getConnect();

        //获取services的子节点信息并监听，从中获取服务器列表
        client.getServiceList();
        client.handleBussiness();

    }
}
