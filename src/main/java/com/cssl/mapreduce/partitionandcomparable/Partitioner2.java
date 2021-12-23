package com.cssl.mapreduce.partitionandcomparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Partitioner2 extends Partitioner<FlowBean, Text> {

    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {

        String s = text.toString();

        String substring = s.substring(0, 3);

        int part;

        if ("136".equals(substring)){
            part=0;
        }else if ("137".equals(substring)){
            part=1;
        }else if ("138".equals(substring)){
            part=2;
        }else if ("139".equals(substring)){
            part=3;
        }else {
            part=4;
        }
        return part;
    }
}
