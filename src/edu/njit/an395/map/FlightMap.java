package edu.njit.an395.map;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.njit.an395.RawFlight;
import edu.njit.an395.hadoop.HadoopFlight;
import edu.njit.an395.util.FlightUtil;

public class FlightMap extends Mapper<LongWritable, Text, Text, HadoopFlight> {

	private Text flightSymbol = new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (!StringUtils.startsWith(value.toString(), "Year")) {
			List<String> attributes = Arrays.asList(value.toString().split(","));
			RawFlight aRawFlight = FlightUtil.convert(attributes);
			HadoopFlight aHadoopFlight = FlightUtil.convert(aRawFlight);
			flightSymbol.set(aHadoopFlight.getUniqueCarrier());
			context.write(flightSymbol, aHadoopFlight);
		}
	}
}
