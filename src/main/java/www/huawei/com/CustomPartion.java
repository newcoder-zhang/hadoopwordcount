package www.huawei.com;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartion extends Partitioner<CustomWritable,IntWritable> {
    public int getPartition(CustomWritable customWritable, IntWritable intWritable, int i) {
        return 0;
    }
}
