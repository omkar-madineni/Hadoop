/**
 * 
 */
package com.cloudwick.hadoop.mapreduce.average;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Omkar
 *
 */
public class AverageWordDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Average of Words <in> <out>");
			System.exit(2);
		}

		Configuration conf = getConf();
		Job job = Job.getInstance(conf);

		job.setJobName("word count");
		job.setJarByClass(AverageWordDriver.class);
		job.setMapperClass(AverageWordMapper.class);
		job.setCombinerClass(AverageWordCombiner.class);
		job.setReducerClass(AverageWordReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		int ret = job.waitForCompletion(true) ? 0 : 1;

		return ret;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner
				.run(new Configuration(), new AverageWordDriver(), args);
		System.exit(res);
	}

}
