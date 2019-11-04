package flume;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

/**
 * @program:bigdata
 * @package:flume
 * @filename:myHdfsSink.java
 * @create:2019.10.21.14.41
 * @author:Administrator
 * @descrption.自定义sink
 */
public class myHdfsSink extends AbstractSink implements Configurable {


    @Override
    public Status process() throws EventDeliveryException {
        return null;
    }

    @Override
    public void configure(Context context) {

    }
}
