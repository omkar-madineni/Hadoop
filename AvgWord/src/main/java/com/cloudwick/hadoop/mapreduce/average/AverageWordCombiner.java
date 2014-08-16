/**
 * 
 */
package com.cloudwick.hadoop.mapreduce.average;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Omkar
 *
 */
public class AverageWordCombiner extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable inter = new IntWritable();

	public void reduce(Text Key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values)
			sum += value.get();
		inter.set(sum);
		context.write(Key, inter);
	}

}
