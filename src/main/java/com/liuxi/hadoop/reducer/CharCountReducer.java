package com.liuxi.hadoop.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @Auther: liuxi
 * @Date: 2019/4/2 11:08
 * @Description:
 */
public class CharCountReducer extends org.apache.hadoop.mapreduce.Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long count = 0;
        for (LongWritable value : values) {
            count += value.get();
        }
        context.write(key, new LongWritable(count));
    }

}
