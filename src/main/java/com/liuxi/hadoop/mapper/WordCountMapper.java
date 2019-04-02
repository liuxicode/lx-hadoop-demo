package com.liuxi.hadoop.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @Auther: liuxi
 * @Date: 2019/4/2 16:54
 * @Description:
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer stringTokenizer = new StringTokenizer(value.toString());

        while (stringTokenizer.hasMoreTokens()) {
            String word = stringTokenizer.nextToken();

            context.write(new Text(word),new LongWritable(1));
        }
    }

}
