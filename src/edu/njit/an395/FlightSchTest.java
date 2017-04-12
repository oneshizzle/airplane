package edu.njit.an395;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import edu.njit.an395.hadoop.FlightScheduleStats;

public class FlightSchTest {

	public static void main(String[] args) {
		FlightScheduleStats stats1 = new FlightScheduleStats();
		FlightScheduleStats stats2 = new FlightScheduleStats();
		FlightScheduleStats stats3 = new FlightScheduleStats();

		stats1.setUniqueCarrier(new Text("Small"));
		stats1.setLateProbability(new DoubleWritable(1.0));
		stats2.setUniqueCarrier(new Text("Med"));
		stats2.setLateProbability(new DoubleWritable(50.0));
		stats3.setUniqueCarrier(new Text("Big"));
		stats3.setLateProbability(new DoubleWritable(99.0));

		List<FlightScheduleStats> list = new ArrayList<FlightScheduleStats>();
		list.add(stats2);
		list.add(stats3);
		list.add(stats1);
		/***
		 * list.forEach(item -> System.out.println(item));
		 * Collections.sort(list); list.forEach(item ->
		 * System.out.println(item)); Collections.reverse(list);
		 * list.forEach(item -> System.out.println(item));
		 */
	}

}
