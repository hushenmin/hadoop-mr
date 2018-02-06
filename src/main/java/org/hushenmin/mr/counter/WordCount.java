package org.hushenmin.mr.counter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by shenmin on 2018/2/6.
 */
public class WordCount extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.hdfs.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
        int result = ToolRunner.run(conf, new WordCount(), args);
    }

    public int run(String[] args) throws Exception {

        Job  job = Job.getInstance(super.getConf(), WordCount.class.getSimpleName());
        FileInputFormat.addInputPath(job,new Path(args[0]));
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setJarByClass(WordCount.class);
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        boolean result = job.waitForCompletion(true);
        return  result ? 0:1;
    }
    public static class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        IntWritable v2 =  new IntWritable(1);
        Text k2 = new Text();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            //super.map(key, value, context);
            context.getCounter("mr_counter","map_input_kv_counter").increment(1);
            String[] strings = v1.toString().split("\t");
            for (String str :strings){
                k2.set(str);
                context.write(k2 , v2);
            }

        }
    }
    public static enum Counter{
        reduce_kv_output_counter
    }
    public static class  MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        Text k3 = new Text();
        IntWritable v3 = new IntWritable();
        @Override
        protected void reduce(Text k2, Iterable<IntWritable> v2, Context context) throws IOException, InterruptedException {
            //super.reduce(key, values, context);
           context.getCounter(Counter.reduce_kv_output_counter).increment(1);
            int sum = 0;
            for (IntWritable i : v2){
                sum +=i.get();
            }
            v3.set(sum);
            k3.set(k2);

        }
    }
}
