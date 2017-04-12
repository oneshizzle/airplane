package edu.njit.an395.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.njit.an395.hadoop.AirportTaxi;

public class AvgAirportTaxiReduce extends Reducer<Text, AirportTaxi, AirportTaxi, Text> {

	private List<AirportTaxi> highestList;
	private List<AirportTaxi> lowestList;

	private long count(Iterable<AirportTaxi> taxis) {
		long count = 0;
		Iterator<AirportTaxi> iter = taxis.iterator();
		while (iter.hasNext()) {
			count += 1;
		}
		return count;
	}

	private long sum(Iterable<AirportTaxi> taxis) {
		Long sum = 0L;
		for (AirportTaxi taxi : taxis) {
			sum += new Long(taxi.getTaxiTime().get());
		}
		return sum.longValue();
	}

	@Override
	protected void reduce(Text airport, Iterable<AirportTaxi> airportTaxis, Context context) throws IOException, InterruptedException {
		if (null == highestList) {
			highestList = new ArrayList<AirportTaxi>(5);
		}

		if (null == lowestList) {
			lowestList = new ArrayList<AirportTaxi>(5);
		}

		AirportTaxi airportTaxi = new AirportTaxi();
		airportTaxi.setAirport(airport);
		long count = count(airportTaxis);
		long sum = sum(airportTaxis);
		airportTaxi.setAverageTaxiTime(new DoubleWritable(sum / count));

		lowestList.add(airportTaxi);
		highestList.add(airportTaxi);

		Collections.sort(lowestList);

		Collections.sort(highestList);
		Collections.reverse(highestList);

		if (highestList.size() > 3) {
			highestList.remove(3);
		}
		if (lowestList.size() > 3) {
			lowestList.remove(3);
		}

	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (AirportTaxi value : highestList) {
			context.write(value, new Text(value.getAverageTaxiTime().toString()));
		}
		for (AirportTaxi value : lowestList) {
			context.write(value, new Text(value.getAverageTaxiTime().toString()));
		}
	}
}
