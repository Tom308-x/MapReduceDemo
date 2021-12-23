package com.cssl.mapreduce.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WebLogMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、获取一行
        String line = value.toString();

        //3、etl清洗
        boolean res=parseLog(line,context);
        if (!res){
            return;
        }
        //4、写出
        context.write(value,NullWritable.get());
    }

    private boolean parseLog(String line, Context context) {
        boolean res=false;
        String[] split = line.split(" ");

      //  if (split[0].matches("")) 匹配正则 高级筛选

        if (split.length>11){ //清洗掉日志中长度小于11的数据
            res=true;
        }else {
            res=false;
        }
        return res;
    }
}
