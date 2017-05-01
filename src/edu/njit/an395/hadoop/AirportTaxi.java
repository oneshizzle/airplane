package edu.njit.an395.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class AirportTaxi implements Writable, WritableComparable<AirportTaxi> {

	private Text airport = new Text();
	private ShortWritable taxiTime = new ShortWritable();
	private Text taxiDirection = new Text();
	private DoubleWritable averageTaxiTime = new DoubleWritable(0.0);

	public Text getAirport() {
		return airport;
	}

	public void setAirport(Text airport) {
		this.airport = airport;
	}

	public Text getTaxiDirection() {
		return taxiDirection;
	}

	public void setTaxiDirection(Text taxiDirection) {
		this.taxiDirection = taxiDirection;
	}

	public ShortWritable getTaxiTime() {
		return taxiTime;
	}

	public void setTaxiTime(ShortWritable taxiTime) {
		this.taxiTime = taxiTime;
	}

	public DoubleWritable getAverageTaxiTime() {
		return averageTaxiTime;
	}

	public void setAverageTaxiTime(DoubleWritable averageTaxiTime) {
		this.averageTaxiTime = averageTaxiTime;
	}

	@Override
	public int compareTo(AirportTaxi other) {
		if (null == other)
			return 0;
		if (this.getAverageTaxiTime().get() < other.getAverageTaxiTime().get())
			return -1;
		else
			return 1;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		airport.readFields(dataInput);
		taxiDirection.readFields(dataInput);
		taxiTime.readFields(dataInput);// flight number
		averageTaxiTime.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		airport.write(dataOutput);
		taxiDirection.write(dataOutput);
		taxiTime.write(dataOutput);// flight number
		averageTaxiTime.write(dataOutput);
	}

	public String toString() {
		return "TAXI " + taxiDirection + " for " + airport.toString() + " ";
	}
}
