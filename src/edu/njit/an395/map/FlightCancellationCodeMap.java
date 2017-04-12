package edu.njit.an395.map;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.njit.an395.RawFlight;
import edu.njit.an395.hadoop.HadoopFlight;
import edu.njit.an395.util.FlightUtil;

public class FlightCancellationCodeMap extends Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		List<String> attributes = Arrays.asList(value.toString().split(","));
		RawFlight aRawFlight = FlightUtil.convert(attributes);
		HadoopFlight aHadoopFlight = FlightUtil.convert(aRawFlight);
		if (aHadoopFlight.getCancelled().get() == true) {
			context.write(new Text(aHadoopFlight.getCancellationCode()), new LongWritable(1));
		}
	}
}
