package edu.njit.an395.map;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.njit.an395.RawFlight;
import edu.njit.an395.hadoop.AirportTaxi;
import edu.njit.an395.hadoop.HadoopFlight;
import edu.njit.an395.util.FlightUtil;

public class AirportTaxiInMap extends Mapper<LongWritable, Text, Text, AirportTaxi> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (!StringUtils.startsWith(value.toString(), "Year")) {

			List<String> attributes = Arrays.asList(value.toString().split(","));
			RawFlight aRawFlight = FlightUtil.convert(attributes);
			HadoopFlight aHadoopFlight = FlightUtil.convert(aRawFlight);
			if (!attributes.get(19).trim().equalsIgnoreCase("NA")) {
				AirportTaxi airportTaxiIn = new AirportTaxi();
				airportTaxiIn.setAirport(aHadoopFlight.getDestination());
				airportTaxiIn.setTaxiDirection(new Text("IN"));
				airportTaxiIn.setTaxiTime(aHadoopFlight.getTaxiIn());
				context.write(aHadoopFlight.getDestination(), airportTaxiIn);
			}
		}
	}
}
