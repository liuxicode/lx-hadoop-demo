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
 * @Date: 2019/4/2 10:56
 * @Description:
 */
public class CharCountRunner {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        /**
         * 初始化了一个空的Configuration，但是并没有进行任何的配置，
         * 其实当我们将其运行在一个运行着hadoop的机器上时，它会默认使用我们机器上的配置。
         */
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        //Hadoop 会自动根据驱动程序类路径来扫描该作业的jar包
        job.setJarByClass(com.liuxi.hadoop.CharCountRunner.class);

        // 指定mapper
        job.setMapperClass(com.liuxi.hadoop.mapper.CharCountMapper.class);

        // 指定reducer
        job.setReducerClass(com.liuxi.hadoop.reducer.CharCountReducer.class);

        //map程序的输出键-值对类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //输出键-值对类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //输入文件路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));

        //输出文件路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean res = job.waitForCompletion(true);

        System.exit(res ? 0 : 1);
    }

}
