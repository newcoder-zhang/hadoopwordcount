package www.huawei.com;

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

public class WordCountMapRed {

    public static class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words =value.toString().split(" ");
            for (String word:words) {
                context.write(new Text(word),new IntWritable(1));

            }
        }
    }

    public static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
           int sum=0;
            for (IntWritable value:values
                 ) {
                sum+=value.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

    public  int run(String[] args) throws Exception {
        Job job=Job.getInstance(new Configuration(),"wordcount");
        job.setJarByClass(getClass());

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        return job.waitForCompletion(true)?0:1;

    }

    public static void main(String[] args) throws Exception{
        int r=new WordCountMapRed().run(args);
        System.exit(r);
    }
}
