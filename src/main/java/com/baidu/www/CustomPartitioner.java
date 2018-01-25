package com.baidu.www;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区规则不变
 */
public class CustomPartitioner extends Partitioner<TextIntWritable,IntWritable> {
    public int getPartition(TextIntWritable key, IntWritable value, int i) {
        return key.getFirst().hashCode()%i;
    }
}
