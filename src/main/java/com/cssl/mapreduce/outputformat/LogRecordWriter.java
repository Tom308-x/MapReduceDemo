package com.cssl.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private   FSDataOutputStream otherOut;
    private   FSDataOutputStream atguiguOut;

    public LogRecordWriter(TaskAttemptContext job) {

        //创建两条流
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            atguiguOut = fs.create(new Path("D:\\hadoopTest\\atguigu.log"));
            otherOut = fs.create(new Path("D:\\hadoopTest\\other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {

        String log = key.toString();
        //具体写出
        if (log.contains("atguigu")){
            atguiguOut.writeBytes(log+"\n");
        }else {
            otherOut.writeBytes(log+"\n");
        }

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        //关闭流
        IOUtils.closeStream(atguiguOut);
        IOUtils.closeStream(otherOut);
    }
}
