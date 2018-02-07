package org.hushenmin.mr.secondsort;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * Created by Administrator on 2018/2/7.
 */
public class SecondSortMapReduce {
    public static class MySendarySortMap extends Mapper<LongWritable,Text,CombinationKey,IntWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key ==null || value == null ||key.toString().equals("")){
                return;
            }
            String[] values = value.toString().split("\t");
            context.write(new CombinationKey(values[0],Integer.valueOf(values[1])),new IntWritable(Integer.valueOf(values[1])));
        }
    }
    public static class  MySendarySortReducer extends Reducer<CombinationKey,IntWritable,Text,Text>{

        @Override
        protected void reduce(CombinationKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for ( IntWritable v:values){
                context.write(new Text(key.getFirstKey()),new Text(v.toString()));
            }
        }
    }

    public static void main(String[] args) {

            try {
                // 创建配置信息
                Configuration conf = new Configuration();
               // conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "\t");

                /*// 创建文件系统
                //FileSystem fileSystem = FileSystem.get(new URI(args[0]), conf);
                // 如果输出目录存在，我们就删除
                if (fileSystem.exists(new Path(args[1]))) {
                    fileSystem.delete(new Path(args[1]), true);
                }*/

                // 创建任务
                Job job = new Job(conf, SecondSortMapReduce.class.getName());
                job.setJarByClass(SecondSortMapReduce.class);

                //1.1   设置输入目录和设置输入数据格式化的类
                FileInputFormat.setInputPaths(job, args[0]);
                //job.setInputFormatClass(KeyValueTextInputFormat.class);

                //1.2   设置自定义Mapper类和设置map函数输出数据的key和value的类型
                job.setMapperClass(MySendarySortMap.class);
                job.setMapOutputKeyClass(CombinationKey.class);
                job.setMapOutputValueClass(IntWritable.class);

                //1.3   设置分区和reduce数量(reduce的数量，和分区的数量对应，因为分区为一个，所以reduce的数量也是一个)
                job.setPartitionerClass(DefinedPartition.class);
                job.setNumReduceTasks(1);

                //设置自定义分组策略
                job.setGroupingComparatorClass(DefinedGroupSort.class);
                //设置自定义比较策略(因为我的CombineKey重写了compareTo方法，所以这个可以省略)
                job.setSortComparatorClass(DefinedComparator.class);

                //1.4   排序
                //1.5   归约
                //2.1   Shuffle把数据从Map端拷贝到Reduce端。
                //2.2   指定Reducer类和输出key和value的类型
                job.setReducerClass(MySendarySortReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                //2.3   指定输出的路径和设置输出的格式化类
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                //job.setOutputFormatClass(TextOutputFormat.class);


                // 提交作业 退出
                System.exit(job.waitForCompletion(true) ? 0 : 1);

            } catch (Exception e) {
                e.printStackTrace();
            }

    }
}
