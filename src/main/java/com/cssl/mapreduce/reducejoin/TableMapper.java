package com.cssl.mapreduce.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {


    private String filename;
    private Text outK=new Text();
    private TableBean outV=new TableBean();
    //初始化
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //获取切片文件信息
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        //获取文件名称
         filename = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();

        //判断数据来源是那个文件
        if (filename.contains("order")){
            //切割数据成为table中的属性存放在数组中
            String[] split = line.split("\t");
            //封装
            outK.set(split[1]);
            outV.setId(split[0]);
            outV.setPid(split[1]);
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setPname("");
            outV.setFlag("order");
        }else {
            String[] split = line.split("\t");
            outK.set(split[0]);
            outV.setId("");
            outV.setPid(split[0]);
            outV.setAmount(0);
            outV.setPname(split[1]);
            outV.setFlag("pd");
        }

        //写出
        context.write(outK,outV);
    }
}
