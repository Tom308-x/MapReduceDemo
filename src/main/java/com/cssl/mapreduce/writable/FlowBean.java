package com.cssl.mapreduce.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
* 1、定义类实现writable接口
* 2、重写序列化和反序列化方法
* 3、重写空参构造
* 4、tostring方法
* */
public class FlowBean implements Writable{


    private Long upFlow;        //上行流量
    private Long downFlow;      //下行流量
    private Long sumFlow;       //总流量

    //无参构造器
    public FlowBean() {
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }
    public void setSumFlow() {
        this.sumFlow = this.downFlow+this.upFlow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow=in.readLong();
        this.downFlow=in.readLong();
        this.sumFlow=in.readLong();
    }


    @Override
    public String toString() {
        return upFlow +"\t" + downFlow + "\t" + sumFlow;
    }
}
