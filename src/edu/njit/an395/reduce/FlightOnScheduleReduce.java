package edu.njit.an395.reduce;

import java.io.IOException;
import java.util.ArrayList;
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
		System.out.println(carrier + " ADRIEN");
		if (null == highestList) {
			highestList = new ArrayList<FlightScheduleStats>();
		}

		if (null == lowestList) {
			lowestList = new ArrayList<FlightScheduleStats>();
		}

		FlightScheduleStats lowest = new FlightScheduleStats();
		lowest.setUniqueCarrier(carrier);
		long lateTotal = lateSum(flights);
		long total = sum(flights);
		lowest.setLateProbability(new DoubleWritable(lateTotal / total));

		FlightScheduleStats highest = new FlightScheduleStats();
		highest.setUniqueCarrier(carrier);
		lateTotal = lateSum(flights);
		total = sum(flights);
		highest.setLateProbability(new DoubleWritable(lateTotal / total));

		lowestList.add(lowest);
		highestList.add(highest);
		
		System.out.println(lowestList.size() + " " + highestList.size());
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
