package zookeeper.bruip;

        import org.apache.zookeeper.*;
        import org.apache.zookeeper.data.Stat;

/**
 * @program:bigdata
 * @package:zookeeper.bruip
 * @filename:zKOperate.java
 * @create:2019.10.16.14.14
 * @author:Administrator
 * @descrption.对于节点的增删改查,zk同时提供了同步和异步的API
 */
public class zKOperate {
    private ZooKeeper zk;
    public zKOperate(){
        ZkWatcher zw = new ZkWatcher("zk01:2181,zk02:2181,zk03:2181");
        zw.connect();
        this.zk=zw.getZk();
        long sid = this.zk.getSessionId();
        System.out.println("本次会话的ID为"+sid);
    }
    //创建节点：create /abc "数据"
    public void create(String nodePath,String data,boolean sync) throws KeeperException, InterruptedException {
        if (sync){//同步创建
            String path = this.zk.create(nodePath, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("同步创建的节点："+path);
        }else {//异步创建
            this.zk.create(nodePath, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                    new AsyncCallback.Create2Callback() {
                        @Override
                        public void processResult(int i, String s, Object o, String s1, Stat stat) {//Stat为元数据对象
                            System.out.println("异步创建的执行线程"+Thread.currentThread().getName());
                            KeeperException.Code code = KeeperException.Code.get(i);
                            System.out.println("异步创建的返回码："+code);
                            System.out.println("异步创建的节点的路径："+s);
                            System.out.println("异步创建的节点名称："+s1);
                            System.out.println("主线程传递的参数："+o);
                            System.out.println("异步创建的节点的元数据："+stat);
                        }
                    },null
            );
        }
    }

    //删除节点：delete /abc,deleteAll /
    public void delete(String nodePath,boolean sync) throws KeeperException, InterruptedException {
        if(sync){//同步删除
            this.zk.delete(nodePath,-1);
        }else {//异步删除
            this.zk.delete(nodePath, -1, new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(int i, String s, Object o) {
                    KeeperException.Code code = KeeperException.Code.get(i);
                    if (code==KeeperException.Code.OK){
                        System.out.println("节点"+s+"删除成功");
                    }
                    if (code==KeeperException.Code.NONODE){
                        System.out.println("节点"+s+"不存在，删除失败");
                    }
                    if (code==KeeperException.Code.NOTEMPTY){
                        System.out.println("节点"+s+"非空，删除失败");
                    }
                }
            },null);
        }
    }

    //获取节点数据
    public void getdata(String nodePath , boolean sync  ) throws KeeperException, InterruptedException {
        if (sync){
            Stat stat = new Stat();
            byte[] data = this.zk.getData(nodePath, false,stat);
            System.out.println("同步节点获取的数据"+new String(data));
            System.out.println("同步节点获取的元数据"+stat);
        }else {
            this.zk.getData(nodePath, false, new AsyncCallback.DataCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                    KeeperException.Code code = KeeperException.Code.get(rc);
                    System.out.println("异步获取的执行结果"+code );
                    System.out.println("异步获取的节点"+path );
                    System.out.println("异步获取的数据"+new String(data) );
                    System.out.println("异步获取的元数据"+stat );
                }
            },null);
        }
    }

    //修改数据
    public void setdata(String nodePath,String data ,boolean sync   ) throws KeeperException, InterruptedException {
        if (sync){
            Stat stat = this.zk.setData(nodePath, data.getBytes(), -1);
            System.out.println("修改后的版本"+stat.getVersion());
        }else {
            this.zk.setData(nodePath, data.getBytes(), -1, new AsyncCallback.StatCallback() {
                @Override
                public void processResult(int i, String s, Object o, Stat stat) {
                    System.out.println("异步修改后的版本"+stat.getVersion());
                }
            },null);
        }
    }



    public static void main(String[] args) throws KeeperException, InterruptedException {
        zKOperate zo = new zKOperate();
        zo.create("/bd1903/123","bd1903",true);
        zo.create("/bd1903/456","bd1903",false);
    }
}

