package edu.njit.an395.combine;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.njit.an395.hadoop.HadoopFlight;

public class FlightCombine extends Reducer<HadoopFlight, LongWritable, HadoopFlight, LongWritable> {

	@Override
	protected void reduce(HadoopFlight key, Iterable<LongWritable> values, Context context) throws IOException,
			InterruptedException {
		int count = 0;
		for (LongWritable value : values) {
			count += value.get();
		}
		// System.out.println("LINE 85: " + key + ":" + count);
		context.write(key, new LongWritable(count));
	}
}
