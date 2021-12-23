package com.cssl.mapreduce.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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
        job.setMapOutputValueClass(IntWritable.class);
        //5、设置最终输出的K、V类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        job.setCombinerClass(WordCountCombiner.class);
        job.setCombinerClass(WordCountReducer.class);
        //6、设置文件的输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path("hdfs://localhost:9000/input/inputword/hello.txt"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://localhost:9000/out222"));
//        FileInputFormat.setInputPaths(job,new Path(args[0]));
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //7、提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
