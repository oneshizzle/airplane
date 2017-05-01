package edu.njit.an395.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class FlightScheduleStats implements Writable, WritableComparable<FlightScheduleStats> {

	private Text uniqueCarrier;// unique carrier code
	// probability of being late
	private DoubleWritable lateProbability  = new DoubleWritable(0.0);

	public Text getUniqueCarrier() {
		return uniqueCarrier;
	}

	public void setUniqueCarrier(Text uniqueCarrier) {
		this.uniqueCarrier = uniqueCarrier;
	}

	public DoubleWritable getLateProbability() {
		return lateProbability;
	}

	public void setLateProbability(DoubleWritable probability) {
		this.lateProbability = probability;
	}

	@Override
	public int compareTo(FlightScheduleStats next) {
		if (null == next)
			return 0;
		if (this.getLateProbability().get() < next.getLateProbability().get())
			return -1;
		else
			return 1;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		uniqueCarrier.readFields(dataInput);
		lateProbability.readFields(dataInput);// flight number
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		uniqueCarrier.write(dataOutput);
		lateProbability.write(dataOutput);// flight number
	}

	@Override
	public int hashCode() {
		int result = (this.uniqueCarrier != null ? this.uniqueCarrier.hashCode() : 0) + 13;
		return result;
	}

	@Override
	public boolean equals(Object that) {
		if (null == that || getClass() != that.getClass())
			return false;

		if (this == that)
			return true;

		FlightScheduleStats flight = (FlightScheduleStats) that;

		if (flight != null && !this.uniqueCarrier.equals(flight.getUniqueCarrier()))
			return false;

		return true;
	}

	public String toString() {
		return this.getUniqueCarrier() + " (" + this.getLateProbability() + ") ";
	}

}
