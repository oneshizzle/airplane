package edu.njit.an395.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.njit.an395.hadoop.FlightScheduleStats;
import edu.njit.an395.hadoop.HadoopFlight;

public class FlightOnScheduleReduce extends Reducer<Text, HadoopFlight, FlightScheduleStats, Text> {

	private List<FlightScheduleStats> highestList;
	private List<FlightScheduleStats> lowestList;

	protected void reduce(Text carrier, Iterable<HadoopFlight> flights, Context context) throws IOException, InterruptedException {

		double lateTotal = 0.0;
		double total = 0.0;

		for (HadoopFlight flight : flights) {
			total = total + 1;
			if (flight.isOnSchedule() == false) {
				lateTotal = lateTotal + 1;
			}
		}

		if (null == highestList) {
			highestList = new ArrayList<FlightScheduleStats>();
		}

		if (null == lowestList) {
			lowestList = new ArrayList<FlightScheduleStats>();
		}

		Text carrier_name = new Text(carrier);

		FlightScheduleStats lowest = new FlightScheduleStats();
		lowest.setUniqueCarrier(carrier_name);
		lowest.setLateProbability(new DoubleWritable(lateTotal / total));

		FlightScheduleStats highest = new FlightScheduleStats();
		highest.setUniqueCarrier(carrier_name);
		highest.setLateProbability(new DoubleWritable(lateTotal / total));

		// System.out.println(carrier_name + " " + (lateTotal / total) + " LAWRENCE " + lowestList.size() + " " + highestList.size());

		lowestList.add(lowest);
		highestList.add(highest);

		Collections.sort(highestList);
		Collections.sort(lowestList);
		Collections.reverse(lowestList);

		if (lowestList.size() > 5) {
			lowestList.remove(5);
		}

		if (highestList.size() > 5) {
			highestList.remove(5);
		}

	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		int count = 0;
		for (FlightScheduleStats value : highestList) {
			context.write(value, new Text(" EARLY: " + (++count)));
		}

		count = 0;
		for (FlightScheduleStats value : lowestList) {
			context.write(value, new Text(" LATE: " + (++count)));
		}
	}
}
