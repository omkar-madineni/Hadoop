/**
 * 
 */
package com.cloudwick.mapreduce.logcounters;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Omkar
 *
 */
public class LogsCountMapper extends
Mapper<Object, Text , Text, IntWritable>{
	public enum LogCounters{
		Five, Four, Two
	}
	private String status;
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		String[] fields = line.split(" ");
		status = fields[7];
		if(status=="200")
			context.getCounter(LogCounters.Two).increment(1);
		else if(status=="404")
			context.getCounter(LogCounters.Four).increment(1);
		else if(status=="503")
			context.getCounter(LogCounters.Five).increment(1);
			
	}

}
