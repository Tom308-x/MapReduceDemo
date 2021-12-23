package com.cssl.mapreduce.combinetextinputformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
* KEYIN:map阶段输入key的类型：LongWritable
* VALUEIN:map阶段输入value的类型：text
* KEYOUT: map阶段输出的key类型：Text
* VALUEOUT:map阶段输出的value类型：IntWritable
* */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private Text outK=new Text();
    private IntWritable outV=new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1、获取一行
        String line = value.toString();

        //2、切割
        String[] words = line.split(" ");

        //3、循环写出
        for (String word:words) {
            //封装outK
            outK.set(word);
            context.write(outK,outV);
        }
    }
}
