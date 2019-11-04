/**
 * @program:bigdata
 * @package:PACKAGE_NAME
 * @filename:TestVolatile.java
 * @create:2019.09.20.09.47
 * @author:Administrator
 * @descrption.测试vlolatile关键字
 */
public class TestVolatile {
    public static volatile int numb = 0;
    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 100; i++) {
            new Thread(new Runnable() {
                public void run() {
                    for (int i = 0; i < 1000; i++) {
                        numb++;
                    }
                }
            }).start();
        }
        Thread.sleep(2000);
        System.out.println(numb);
    }
}
