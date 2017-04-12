package edu.njit.an395.util;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;

import edu.njit.an395.RawFlight;
import edu.njit.an395.hadoop.HadoopFlight;

public class FlightUtil {

	public static HadoopFlight convert(RawFlight aRawFlight) {
		HadoopFlight aHadoopFlight = new HadoopFlight();
		try {
			aHadoopFlight.setUniqueCarrier(new Text(aRawFlight.getUniqueCarrier()));
			aHadoopFlight.setArrivalDelay(new ShortWritable(aRawFlight.getArrivalDelay()));
			aHadoopFlight.setDepartureDelay(new ShortWritable(aRawFlight.getDepartureDelay()));
			aHadoopFlight.setCancellationCode(new Text(aRawFlight.getCancellationCode()));
			aHadoopFlight.setCancelled(new BooleanWritable((aRawFlight.getCancelled() == 1)));
			aHadoopFlight.setDestination(new Text(aRawFlight.getDestination()));
			aHadoopFlight.setOrigin(new Text(aRawFlight.getOrigin()));
			aHadoopFlight.setTaxiIn(new ShortWritable(convert(aRawFlight.getTaxiIn())));
			aHadoopFlight.setTaxiOut(new ShortWritable(convert(aRawFlight.getTaxiOut())));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return aHadoopFlight;
	}

	public static RawFlight convert(List<String> attributes) {
		RawFlight aRawFlight = new RawFlight();
		aRawFlight.setYear(attributes.get(0));
		aRawFlight.setMonth(attributes.get(1));
		aRawFlight.setDayOfMonth(attributes.get(2));
		aRawFlight.setDayOfWeek(attributes.get(3));
		aRawFlight.setActualDepTime(attributes.get(4));
		aRawFlight.setListedDepTime(attributes.get(5));
		aRawFlight.setActualArrivalTime(attributes.get(6));
		aRawFlight.setListedArrTime(attributes.get(7));
		aRawFlight.setUniqueCarrier(attributes.get(8));
		aRawFlight.setFlightNum(attributes.get(9));
		aRawFlight.setTailNum(attributes.get(10));
		aRawFlight.setActualElapsedTime(convert(attributes.get(11)));
		aRawFlight.setListedElapsedTime(convert(attributes.get(12)));
		aRawFlight.setAirTime(convert(attributes.get(13)));
		aRawFlight.setArrivalDelay(convert(attributes.get(14)));
		aRawFlight.setDepartureDelay(convert(attributes.get(15)));
		aRawFlight.setOrigin(attributes.get(16));
		aRawFlight.setDestination(attributes.get(17));
		aRawFlight.setDistance(convert(attributes.get(18)));// in miles
		aRawFlight.setTaxiIn(attributes.get(19));
		aRawFlight.setTaxiOut(attributes.get(20));
		aRawFlight.setCancelled(convert(attributes.get(21)));
		aRawFlight.setCancellationCode(attributes.get(22));
		aRawFlight.setDiverted(convert(attributes.get(23)));
		return aRawFlight;
	}

	private static short convert(String aString) {
		if (StringUtils.isNumeric(aString)) {
			return new Short(aString).shortValue();
		} else {
			return -1;
		}
	}
}
