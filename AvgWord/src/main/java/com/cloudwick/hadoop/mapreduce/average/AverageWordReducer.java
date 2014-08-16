/**
 * 
 */
package com.cloudwick.hadoop.mapreduce.average;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Omkar
 *
 */
public class AverageWordReducer extends
		Reducer<Text, IntWritable, Text, DoubleWritable> {
	private DoubleWritable Avg = new DoubleWritable();

	public void reduce(Text Key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		double avg, count = 0.0;
		for (IntWritable value : values) {
			sum += value.get();
			count++;
		}
		avg = sum / (count);
		Avg.set(avg);
		context.write(Key, Avg);
	}
}
