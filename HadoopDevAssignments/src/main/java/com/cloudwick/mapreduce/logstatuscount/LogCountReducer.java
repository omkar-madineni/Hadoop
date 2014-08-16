/**
 * 
 */
package com.cloudwick.mapreduce.logstatuscount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * @author Omkar
 *
 */
public class LogCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable sum = new IntWritable();
	public void reduce(Text Key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		int count = 0;
		for(IntWritable value: values)
			count += value.get();
		sum.set(count);
		context.write(Key, sum);
	}
}
