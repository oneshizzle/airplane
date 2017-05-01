package edu.njit.an395.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.njit.an395.hadoop.AirportTaxi;

public class AvgAirportTaxiReduce extends Reducer<Text, AirportTaxi, AirportTaxi, Text> {

	private List<AirportTaxi> highestList;
	private List<AirportTaxi> lowestList;

	protected void reduce(Text airport, Iterable<AirportTaxi> airportTaxis, Context context) throws IOException, InterruptedException {
		
		if (null == highestList) {
			highestList = new ArrayList<AirportTaxi>(5);
		}

		if (null == lowestList) {
			lowestList = new ArrayList<AirportTaxi>(5);
		}

		Double totalTaxiTime = 0.0;
		String taxiDirection = "";
		Double count = 0.0;
		
		for (AirportTaxi taxi : airportTaxis) {
			taxiDirection = taxi.getTaxiDirection().toString();
			totalTaxiTime += new Double(taxi.getTaxiTime().get());
			count += 1;
		}
		
		String airportName = airport.toString();

		AirportTaxi airportTaxi1 = new AirportTaxi();
		airportTaxi1.setAirport(new Text(airportName));
		airportTaxi1.setTaxiDirection(new Text(taxiDirection));
		airportTaxi1.setTaxiTime(new ShortWritable());
		airportTaxi1.setAverageTaxiTime(new DoubleWritable(totalTaxiTime / count));

		AirportTaxi airportTaxi2 = new AirportTaxi();
		airportTaxi2.setTaxiTime(new ShortWritable());
		airportTaxi2.setTaxiDirection(new Text(taxiDirection));
		airportTaxi2.setAirport(new Text(airportName));
		airportTaxi2.setAverageTaxiTime(new DoubleWritable(totalTaxiTime / count));

		// System.out.println("ADRIEN " + airportName + "[" + totalTaxiTime + "/" + count + "]");

		lowestList.add(airportTaxi1);
		highestList.add(airportTaxi2);

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
