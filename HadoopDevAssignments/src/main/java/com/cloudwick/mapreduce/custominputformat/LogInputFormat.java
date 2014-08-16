/**
 * 
 */
package com.cloudwick.mapreduce.custominputformat;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * @author Omkar
 *
 */
public class LogInputFormat extends InputFormat<LongWritable, LogWritable> {
	
	private TextInputFormat tif = new TextInputFormat();
	@Override
	public RecordReader<LongWritable, LogWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return new LogRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return tif.getSplits(context);
	}

}
