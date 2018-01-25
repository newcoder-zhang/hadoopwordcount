package com.baidu.www;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * 50 1
 * 60 2
 * 50 10
 * 10 2
 * 10 1
 * 20 3
 * 20 4
 * 20 1
 * 30 5
 * 30 1
 */

public class SecondaryWordCountMapRed {

    public static class MyMapper extends Mapper<LongWritable, Text, TextIntWritable, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString();
            String[] split = s.split(" ");
            int x = Integer.valueOf(split[1]);
            TextIntWritable newKey = new TextIntWritable(split[0], x);
            IntWritable newValue = new IntWritable(x);
            context.write(newKey, newValue);
        }
    }

    public static class MyReducer extends Reducer<TextIntWritable, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(TextIntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable i : values
                    ) {
                context.write(new Text(key.getFirst()), i);
            }
        }
    }

    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setMapperClass(MyMapper.class);
        job.setSortComparatorClass(CustomSortComparator.class);
        job.setGroupingComparatorClass(CustomGroupingComparator.class);
        job.setPartitionerClass(CustomPartitioner.class);

        job.setMapOutputKeyClass(TextIntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int r = new SecondaryWordCountMapRed().run(args);
        System.exit(r);
    }
}
