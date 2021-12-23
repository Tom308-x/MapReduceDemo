package com.cssl.mapreduce.writable;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1、获取job
        Job job = Job.getInstance(new Configuration());
        //2、设置jar包
        job.setJarByClass(FlowDriver.class);
        //3、关联mapper，reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReduce.class);
        //4、设置mapper的K、V类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //5、设置最终的K、V类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //6、设置数据的输入和输出路径
        FileInputFormat.setInputPaths(job,new Path("hdfs://localhost:9000/input/inputflow/"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://localhost:9000/out/flowout/"));
        //7、提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }


}

