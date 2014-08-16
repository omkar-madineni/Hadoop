package com.cloudwick.mapreduce.custominputformat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class LogRecordReader extends RecordReader<LongWritable, LogWritable> {

	public Pattern pattern = Pattern
			.compile("^([\\d.]+) (\\S+) (\\S+) \\[([\\S]+)\\] \\\"([^\\\"][\\w]+) ([\\S]+) ([\\S]+)\\\" ([\\d]{3}) ([\\d]+) \\\"([^\\\"]+)\\\" \\\"([^\\\"]+)\\\"");
	private LineRecordReader lrr = new LineRecordReader();
	LogWritable lw = new LogWritable();

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		lrr.close();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return lrr.getCurrentKey();
	}

	@Override
	public LogWritable getCurrentValue() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return lw;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return lrr.getProgress();
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		lrr.initialize(split, context);

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		while (lrr.nextKeyValue()) {
			Matcher matcher;
			matcher = pattern.matcher(lrr.getCurrentValue().toString());
			if (matcher.find()) {
				int fieldCount = matcher.groupCount();
				String[] fields = new String[fieldCount];
				for (int i = 0; i < fieldCount; i++)
					fields[i] = new String(matcher.group(i + 1));
				/*
				 * String ipAddr = fields[0]; String clientIdentity = fields[1];
				 * String userId = fields[2]; String timeStamp = fields[3];
				 * String requestType = fields[4]; String requestPage =
				 * fields[5]; String httpProt = fields[6]; int responseCode =
				 * Integer.parseInt(fields[7]); int responseSize =
				 * Integer.parseInt(fields[8]); String referrer = fields[9];
				 * String userAgent = fields[10]; lw = new LogWritable(ipAddr,
				 * clientIdentity, userId, timeStamp, requestType, requestPage,
				 * httpProt, responseCode, responseSize, referrer, userAgent);
				 */
				lw.setIpAddr(fields[0]);
				lw.setClientIdentity(fields[1]);
				lw.setUserId(fields[2]);
				lw.setTimeStamp(fields[3]);
				lw.setRequestType(fields[4]);
				lw.setRequestPage(fields[5]);
				lw.setResponseCode(Integer.parseInt(fields[7]));
				lw.setRepsonseSize(Integer.parseInt(fields[8]));
				lw.setReferrer(fields[9]);
				lw.setUserAgent(fields[10]);
				return true;
			}
		}
		return false;
	}
}
