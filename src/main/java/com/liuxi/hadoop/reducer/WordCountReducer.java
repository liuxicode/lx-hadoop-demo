package com.liuxi.hadoop.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * @Auther: liuxi
 * @Date: 2019/4/2 17:11
 * @Description:
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long count = 0;

        for (LongWritable value : values) {
            count += value.get();
        }

        context.write(key,new LongWritable(count));
    }
}
