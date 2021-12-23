package com.cssl.mapreduce.combinetextinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1、获取job
        Job job = Job.getInstance(new Configuration());
        //2、设置jar包路径
        job.setJarByClass(WordCountDriver.class);
        //3、关联mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //4、设置map输出的K、V类型
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //5、设置最终输出的K、V类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //如果不设置inputformat，默认会使用TextInputFormat.class
        job.setInputFormatClass(CombineTextInputFormat.class);
        //虚拟存储切片最大值
        CombineTextInputFormat.setMaxInputSplitSize(job,20971520); //20m

        //6、设置文件的输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path("hdfs://localhost:9000/input/inputcombinetextinputformat"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://localhost:9000/out/combinetextinputformat"));

        //7、提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
