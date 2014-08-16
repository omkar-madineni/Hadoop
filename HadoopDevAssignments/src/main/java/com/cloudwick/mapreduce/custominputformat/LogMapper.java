/**
 * 
 */
package com.cloudwick.mapreduce.custominputformat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * @author Omkar
 *
 */
public class LogMapper extends Mapper<Object, LogWritable, Text, IntWritable> {
	private static final IntWritable one = new IntWritable(1);
	private Text status = new Text();
	//LogWritable lws = new LogWritable();
	public void map(Object key, LogWritable value, Context context) throws IOException, InterruptedException{
		status.set(value.getIpAddr());
		context.write(status, one);
	}

}
