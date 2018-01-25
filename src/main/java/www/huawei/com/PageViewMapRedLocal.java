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

/**
 * 在本地测试,不放在yarn上
 */
public class PageViewMapRedLocal{
    public static Text outText=new Text();
    public static IntWritable outInt=new IntWritable();

    public static class PVMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str=value.toString();
            String[] rs = str.split("\t");
            context.getCounter("row","raw info");
            if(rs.length<30){
                return;
            }
            context.getCounter("length >30","bigger 30 info");
            try {
                Integer.parseInt(rs[23]);
            }catch (NumberFormatException e){
                return;
            }
            context.getCounter("intcount","is int");
            outText.set(rs[23]);
            outInt.set(1);
            context.write(outText,outInt);
        }
    }

    public static class PVReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        int sum=0;
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value:values
                 ) {
                int i = value.get();
                sum+=i;
            }
            outInt.set(sum);
            context.write(key,outInt);
        }
    }

    public  int run(String[] args) throws Exception {

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(getClass());

        job.setMapperClass(PVMapper.class);
        job.setReducerClass(PVReducer.class);
      //  job.setCombinerClass(PVReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        FileInputFormat.addInputPath(job,new Path(args[0]));


        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        String input="input/*";
        String output="out19";
        int r=new PageViewMapRedLocal().run(new String[]{input,output});
        //System.exit(r);
    }
}
