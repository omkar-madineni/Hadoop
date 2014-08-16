/**
 * 
 */
package com.cloudwick.mapreduce.custominputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Omkar
 *
 */
public class LogDriver extends Configured implements Tool {

	/**
	 * @param args
	 */
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Log Status Count <in> <out>");
			System.exit(2);
		}
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);

		job.setJobName("Log Count");
		job.setJarByClass(LogDriver.class);
		job.setMapperClass(LogMapper.class);
		// job.setCombinerClass(LogCountCombiner.class);
		job.setReducerClass(LogReducer.class);
		job.setInputFormatClass(LogInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		int ret = job.waitForCompletion(true) ? 0 : 1;

		return ret;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner
				.run(new Configuration(), new LogDriver(), args);
		System.exit(res);

	}

}
