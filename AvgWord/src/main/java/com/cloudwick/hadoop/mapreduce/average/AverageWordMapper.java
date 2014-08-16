/**
 * 
 */
package com.cloudwick.hadoop.mapreduce.average;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Omkar
 *
 */
public class AverageWordMapper extends Mapper<Object, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer str = new StringTokenizer(value.toString());
		while (str.hasMoreTokens()) {
			word.set(str.nextToken());
			context.write(word, one);
		}
	}
}
