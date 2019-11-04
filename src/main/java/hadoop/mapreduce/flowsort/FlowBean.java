package hadoop.mapreduce.flowsort;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @program:bigdata
 * @package:hadoop.mapreduce.flowsum
 * @filename:FlowBean.java
 * @create:2019.09.24.16.56
 * @author:Administrator
 * @descrption.
 */
public class FlowBean implements WritableComparable<FlowBean> {
    private long upFlow;
    private long dFlow;
    private long sumFlow;

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getdFlow() {
        return dFlow;
    }

    public void setdFlow(long dFlow) {
        this.dFlow = dFlow;
    }

    public FlowBean(long upFlow, long dFlow) {
        this.upFlow = upFlow;
        this.dFlow = dFlow;
        this.sumFlow=upFlow+dFlow;
    }
    public void set(long upFlow, long dFlow) {
        this.upFlow = upFlow;
        this.dFlow = dFlow;
        this.sumFlow=upFlow+dFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }
    //反序列化时，需要反射调用空参构造函数，所以需要重新定义一个
    public FlowBean() {
    }

    @Override
    public String toString() {
        return upFlow+"\t"+dFlow+"\t"+sumFlow;
    }

    /*
    * 序列化方法
    * */
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow );
        dataOutput.writeLong(dFlow);
        dataOutput.writeLong(sumFlow);
    }
    /*
     * 反序列化方法
     * */
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readLong();
        dFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();
    }

    public int compareTo(FlowBean o) {
        return this.sumFlow>o.sumFlow?-1:1;
    }

}
