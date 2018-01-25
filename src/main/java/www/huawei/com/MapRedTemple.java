package www.huawei.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MapRedTemple {

    /**mapper
     * Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
     *
     *     结果处理成单个词的<key,value>的形式,在单词统计中,value肯定是1不做任何聚合操作
     *     hadoop 1
     *     hadoop 1
     *     map 1
     *     map 1
     *
     */
    public static class MapRedMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
        }

    }
    /**
     Reducer
     *Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
     *     在reduce之前还有个对单个节点进行merge的shuffle/compile操作,即是相同key合并
     *     hadoop 2
     *     sqoop 4
     *     hdfs 3
     *
     *     这样再传给reducer减轻reducer的压力
     *
     *     reducer再进行最后的合并
     *     hadoop,{2,4,6}
     *     sqoop,{1,1,3}
     */
    public static class MapRedReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
        }
    }

    /**
     * Driver
     *
     */

    public int run(String[] args)throws Exception{
        //获取配置文件
        Configuration configuration=new Configuration();

        //设置job任务
        Job job= Job.getInstance(configuration,getClass().getSimpleName());
        job.setJarByClass(getClass());


        //设置 input
        Path inpath=new Path(args[0]);
        FileInputFormat.addInputPath(job,inpath);

        //设置output
        Path outpath=new Path(args[1]);
        FileOutputFormat.setOutputPath(job,outpath);

        //设置mapper
        job.setMapperClass(MapRedMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

/*        job.setPartitionerClass();
        job.setCombinerClass();
        job.setCombinerKeyGroupingComparatorClass();
        job.setSortComparatorClass();
        job.setGroupingComparatorClass();*/
        //设置reducer
        job.setReducerClass(MapRedReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置yarn
        boolean isSuccess=job.waitForCompletion(true);

        return isSuccess?0:1;

    }

    public static void main(String[] args) throws Exception {
        int status = new MapRedTemple().run(args);

        System.exit(status);
    }
}
