/**
 * 
 */
package com.cloudwick.mapreduce.logstatuscount;

import java.io.IOException;
//import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Omkar
 *
 */
public class LogCountMapper extends
		Mapper<Object, Text , Text, IntWritable> {
	private static final IntWritable one = new IntWritable(1);
	private Text status = new Text();
	//String st[];
	//int a[];
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		//StringTokenizer str = new StringTokenizer(value.toString());
		String[] fields = line.split(" ");
		//while(str.hasMoreElements())
			//st = (String[]) str.nextElement();
		//a[1] = Integer.parseInt(st[8]);
		status.set(fields[7]);
		context.write(status, one);
	}
}
