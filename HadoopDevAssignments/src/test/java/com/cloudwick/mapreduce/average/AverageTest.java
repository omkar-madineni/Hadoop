/**
 * 
 */
package com.cloudwick.mapreduce.average;

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
public class AverageTest {

	/**
	 * @param args
	 */
	MapDriver<Object, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> combinerDriver;
	ReduceDriver<Text, IntWritable, Text, DoubleWritable> reduceDriver;
	MapReduceDriver<Object, Text, Text, IntWritable, Text, DoubleWritable> mrDriver;

	@Before
	public void setUp() {
		AverageMapper mapper = new AverageMapper();
		AverageCombiner combiner = new AverageCombiner();
		AverageReducer reducer = new AverageReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		combinerDriver = ReduceDriver.newReduceDriver(combiner);
		mrDriver = MapReduceDriver
				.newMapReduceDriver(mapper, reducer, combiner);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("abc\tdef"))
				.withOutput(new Text("abc"), new IntWritable(1))
				.withOutput(new Text("def"), new IntWritable(1)).runTest();
	}

	@Test
	public void testCombiner() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));

		combinerDriver.withInput(new Text("abc"), values)
				.withOutput(new Text("abc"), new IntWritable(2)).runTest();
	}

	@Test
	public void testReducer() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(2));
		values.add(new IntWritable(4));

		reduceDriver.withInput(new Text("abc"), values)
				.withOutput(new Text("abc"), new DoubleWritable(3.0)).runTest();
	}

	@Test
	public void testMapReduce() throws IOException {
		mrDriver.withMapper(new AverageMapper())
				.withInput(new LongWritable(), new Text("abc\tdef"))
				.withInput(new LongWritable(), new Text("def\tghi\tabc"))
				.withInput(new LongWritable(), new Text("ghi\tjkl"))
				.withInput(new LongWritable(), new Text("jkl\tabc"))
				.withCombiner(new AverageCombiner())
				.withReducer(new AverageReducer())
				.withOutput(new Text("abc"), new DoubleWritable(3.0))
				.withOutput(new Text("def"), new DoubleWritable(2.0))
				.withOutput(new Text("ghi"), new DoubleWritable(2.0))
				.withOutput(new Text("jkl"), new DoubleWritable(2.0)).runTest();

	}

}
