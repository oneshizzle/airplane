package edu.njit.an395.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.njit.an395.hadoop.FlightScheduleStats;
import edu.njit.an395.hadoop.HadoopFlight;

public class FlightOnScheduleReduce extends Reducer<Text, HadoopFlight, FlightScheduleStats, Text> {

	private List<FlightScheduleStats> highestList;
	private List<FlightScheduleStats> lowestList;

	private long lateSum(Iterable<HadoopFlight> flights) {
		long sum = 0;
		for (HadoopFlight flight : flights) {
			if (flight.isOnSchedule() == false) {
				sum += 1;
			}
		}
		return sum;
	}

	private long sum(Iterable<HadoopFlight> flights) {
		long sum = 0;
		Iterator<HadoopFlight> iter = flights.iterator();
		while (iter.hasNext()) {
			sum += 1;
			iter.next();
		}
		return sum;
	}

	@Override
	protected void reduce(Text carrier, Iterable<HadoopFlight> flights, Context context) throws IOException, InterruptedException {
		if (null == highestList) {
			highestList = new ArrayList<FlightScheduleStats>(10);
		}

		if (null == lowestList) {
			lowestList = new ArrayList<FlightScheduleStats>(10);
		}

		FlightScheduleStats flightStats = new FlightScheduleStats();
		flightStats.setUniqueCarrier(carrier);
		long lateTotal = lateSum(flights);
		long total = sum(flights);
		flightStats.setLateProbability(new DoubleWritable(lateTotal / total));

		lowestList.add(flightStats);
		highestList.add(flightStats);

		Collections.sort(lowestList);

		Collections.sort(highestList);
		Collections.reverse(highestList);

		if (highestList.size() > 5) {
			highestList.remove(5);
		}
		if (lowestList.size() > 5) {
			lowestList.remove(5);
		}

	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (FlightScheduleStats value : highestList) {
			context.write(value, new Text(value.toString()));
		}
		for (FlightScheduleStats value : lowestList) {
			context.write(value, new Text(value.toString()));
		}
	}
}
