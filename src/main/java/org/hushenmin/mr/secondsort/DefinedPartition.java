package org.hushenmin.mr.secondsort;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by Administrator on 2018/2/7.
 */
public class DefinedPartition  extends Partitioner<CombinationKey,IntWritable>{


    public int getPartition(CombinationKey combinationKey, IntWritable intWritable, int num) {
        return  (combinationKey.getFirstKey().hashCode() & Integer.MAX_VALUE) % num;
    }
}
