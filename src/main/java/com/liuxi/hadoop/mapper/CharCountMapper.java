package com.liuxi.hadoop.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @Auther: liuxi
 * @Date: 2019/4/2 10:57
 * @Description:
 */
public class CharCountMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // 将这一行文本转为字符数组
        char[] chars = value.toString().toCharArray();
        for (char c : chars) {
            // 某个字符出现一次，便输出其出现1次。
            context.write(new Text(c + ""), new LongWritable(1));
        }
    }
}
