package com.liuxi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Auther: liuxi
 * @Date: 2019/4/2 19:01
 * @Description:
 */
public class WordCountRunner {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        job.setJarByClass(com.liuxi.hadoop.WordCountRunner.class);

        job.setMapperClass(com.liuxi.hadoop.mapper.WordCountMapper.class);
        job.setCombinerClass(com.liuxi.hadoop.reducer.WordCombiner.class);
        job.setReducerClass(com.liuxi.hadoop.reducer.WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean res = job.waitForCompletion(true);

        System.exit(res ? 0 : 1);
    }

}
