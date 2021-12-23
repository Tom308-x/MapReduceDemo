package com.cssl.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReduce extends Reducer<Text,FlowBean,Text,FlowBean> {

    private FlowBean outV=new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        //1、循环遍历集合，对相同号码的流量进行累加
        Long totalUp=0L;
        Long totalDown=0L;
        for (FlowBean val:values) {
            totalUp+=val.getUpFlow();
            totalDown+=val.getDownFlow();
        }
        //2、封装
        outV.setUpFlow(totalUp);
        outV.setDownFlow(totalDown);
        outV.setSumFlow();

        //3、写出
        context.write(key,outV);
    }
}
