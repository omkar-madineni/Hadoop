/**
 * 
 */
package com.cloudwick.pig.udfs;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.cloudwick.mapreduce.custominputformat.LogInputFormat;
import com.cloudwick.mapreduce.custominputformat.LogRecordReader;
import com.cloudwick.mapreduce.custominputformat.LogWritable;

/**
 * @author Omkar
 *
 */
public class LogLoader extends LoadFunc{
	
	private TupleFactory tf;
	private LogRecordReader rr;
	
	public LogLoader() {
		tf = TupleFactory.getInstance();
	}

	@Override
	public InputFormat getInputFormat() throws IOException {
		// TODO Auto-generated method stub
		return new LogInputFormat();
	}

	@Override
	public Tuple getNext() throws IOException {
		// TODO Auto-generated method stub
		Tuple tuple = null;
		try{
			boolean notDone = rr.nextKeyValue();
			if(!notDone)
				return null;
			LogWritable lw = (LogWritable) rr.getCurrentValue();
			tuple = tf.newTuple(11);
			tuple.set(0, lw.getIpAddr());
			tuple.set(1, lw.getClientIdentity());
			tuple.set(2, lw.getUserId());
			tuple.set(3, lw.getTimeStamp());
			tuple.set(4, lw.getRequestType());
			tuple.set(5, lw.getRequestPage());
			tuple.set(6, lw.getHttpProt());
			tuple.set(7, lw.getResponseCode());
			tuple.set(8, lw.getRepsonseSize());
			tuple.set(9, lw.getReferrer());
			tuple.set(10, lw.getUserAgent());
		}catch(InterruptedException e){
			e.printStackTrace();
		}
		return tuple;
		//return null;
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split)
			throws IOException {
		// TODO Auto-generated method stub
		this.rr = (LogRecordReader)reader;
		
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		// TODO Auto-generated method stub
		FileInputFormat.setInputPaths(job, location);
	}

}
