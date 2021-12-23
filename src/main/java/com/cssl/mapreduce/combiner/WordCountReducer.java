package com.cssl.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
 * KEYIN:Reducer阶段输入key的类型：Text
 * VALUEIN:Reducer阶段输入value的类型：IntWritable
 * KEYOUT: Reducer阶段输出的key类型：Text
 * VALUEOUT:Reducer阶段输出的value类型：IntWritable
 * */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    int sum;
    private IntWritable outV=new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
       sum=0;
        for (IntWritable val:values) {
            //累加统计次数
            System.out.println("val>>>"+val);
            sum+=val.get();
        }
        //写出
        System.out.println("sum>>>"+sum);
        outV.set(sum);
        context.write(key,outV);
    }
}
