/**
 * 
 */
package com.cloudwick.mapreduce.logstatuscount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;


/**
 * @author Omkar
 *
 */
public class LogStatusTest {

	/**
	 * @param args
	 */
	MapDriver<Object, Text, Text, IntWritable> mapDriver;
	//ReduceDriver<Text, IntWritable, Text, IntWritable> combinerDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mrDriver;

	@Before
	public void setUp() {
		LogCountMapper mapper = new LogCountMapper();
		//AverageCombiner combiner = new AverageCombiner();
		LogCountReducer reducer = new LogCountReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		//combinerDriver = ReduceDriver.newReduceDriver(combiner);
		mrDriver = MapReduceDriver
				.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("25.198.250.35 - - [2014-07-19T16:05:33Z] GET / HTTP/1.1 404 1081 - Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)"))
				.withOutput(new Text("404"), new IntWritable(1)).runTest();
	}

	/*@Test
	public void testCombiner() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));

		combinerDriver.withInput(new Text("abc"), values)
				.withOutput(new Text("abc"), new IntWritable(2)).runTest();
	}*/

	@Test
	public void testReducer() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(2));

		reduceDriver.withInput(new Text("404"), values)
				.withOutput(new Text("404"), new IntWritable(2)).runTest();
	}

	@Test
	public void testMapReduce() throws IOException {
		mrDriver.withMapper(new LogCountMapper())
				.withInput(new LongWritable(), new Text("25.198.250.35 - - [2014-07-19T16:05:33Z] GET / HTTP/1.1 344 1081 - Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)"))
				.withInput(new LongWritable(), new Text("25.198.250.35 - - [2014-07-19T16:05:33Z] GET / HTTP/1.1 678 1081 - Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)"))
				.withInput(new LongWritable(), new Text("25.198.250.35 - - [2014-07-19T16:05:33Z] GET / HTTP/1.1 254 1081 - Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)"))
				.withInput(new LongWritable(), new Text("25.198.250.35 - - [2014-07-19T16:05:33Z] GET / HTTP/1.1 404 1081 - Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)"))
				//.withCombiner(new AverageCombiner())
				.withReducer(new LogCountReducer())
				.withOutput(new Text("254"), new IntWritable(1))
				.withOutput(new Text("344"), new IntWritable(1))
				.withOutput(new Text("404"), new IntWritable(1))
				.withOutput(new Text("678"), new IntWritable(1)).runTest();

	}

}
