package com.cssl.mapreduce.partitionandcomparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private FlowBean outK=new FlowBean();
    private Text outV=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取一行
        String s = value.toString();
        //切割
        String[] split = s.split("\t");

        //封装
        outV.set(split[0]);
        outK.setDownFlow(Long.parseLong(split[2]));
        outK.setUpFlow(Long.parseLong(split[1]));
        outK.setSumFlow();

        //写出
        context.write(outK,outV);

    }
}
