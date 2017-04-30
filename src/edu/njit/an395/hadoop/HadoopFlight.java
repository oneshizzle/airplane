package edu.njit.an395.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class HadoopFlight implements Writable {

	private Text flightDate; // 1987-2008
	private Text uniqueCarrier;// unique carrier code
	private Text flightNum;// flight number
	private ShortWritable arrivalDelay;// arrival delay, in minutes
	private ShortWritable departureDelay;// departure delay, in minutes
	private Text origin;// IATA airport code
	private Text destination;// IATA airport code
	private ShortWritable taxiIn;// taxi in time, in minutes
	private ShortWritable taxiOut;// taxi out time in minutes
	private BooleanWritable cancelled;//
	private Text cancellationCode;
	// reason for cancellation (A = carrier, B = weather, C = NAS, D = security)
	private BooleanWritable diverted;// 1 = yes, 0 = no

	/**
	 * @return the flightDate
	 */
	public Text getFlightDate() {
		return flightDate;
	}

	/**
	 * @param flightDate
	 *            the flightDate to set
	 */
	public void setFlightDate(Text flightDate) {
		this.flightDate = flightDate;
	}

	public Text getUniqueCarrier() {
		return uniqueCarrier;
	}

	public void setUniqueCarrier(Text uniqueCarrier) {
		this.uniqueCarrier = uniqueCarrier;
	}

	public Text getFlightNum() {
		return flightNum;
	}

	public void setFlightNum(Text flightNum) {
		this.flightNum = flightNum;
	}

	public ShortWritable getArrivalDelay() {
		return arrivalDelay;
	}

	public void setArrivalDelay(ShortWritable arrivalDelay) {
		this.arrivalDelay = arrivalDelay;
	}

	public ShortWritable getDepartureDelay() {
		return departureDelay;
	}

	public void setDepartureDelay(ShortWritable departureDelay) {
		this.departureDelay = departureDelay;
	}

	public Text getOrigin() {
		return origin;
	}

	public void setOrigin(Text origin) {
		this.origin = origin;
	}

	public Text getDestination() {
		return destination;
	}

	public void setDestination(Text destination) {
		this.destination = destination;
	}

	public ShortWritable getTaxiIn() {
		return taxiIn;
	}

	public void setTaxiIn(ShortWritable taxiIn) {
		this.taxiIn = taxiIn;
	}

	public ShortWritable getTaxiOut() {
		return taxiOut;
	}

	public void setTaxiOut(ShortWritable taxiOut) {
		this.taxiOut = taxiOut;
	}

	public BooleanWritable getCancelled() {
		return cancelled;
	}

	public void setCancelled(BooleanWritable cancelled) {
		this.cancelled = cancelled;
	}

	public Text getCancellationCode() {
		return cancellationCode;
	}

	public void setCancellationCode(Text cancellationCode) {
		this.cancellationCode = new Text("");
		if (cancellationCode.toString().trim().equalsIgnoreCase("A")) {
			this.cancellationCode = new Text("Carrier");
		}
		if (cancellationCode.toString().trim().equalsIgnoreCase("B")) {
			this.cancellationCode = new Text("Weather");
		}

		if (cancellationCode.toString().trim().equalsIgnoreCase("C")) {
			this.cancellationCode = new Text("NAS");
		}

		if (cancellationCode.toString().trim().equalsIgnoreCase("D")) {
			this.cancellationCode = new Text("Security");
		}
	}

	public BooleanWritable isDiverted() {
		return diverted;
	}

	public void setDiverted(BooleanWritable diverted) {
		this.diverted = diverted;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		//flightDate.readFields(dataInput);
		uniqueCarrier.readFields(dataInput);
		flightNum.readFields(dataInput);// flight number
		arrivalDelay.readFields(dataInput);// arrival delay, in minutes
		departureDelay.readFields(dataInput);// departure delay, in minutes
		origin.readFields(dataInput);// IATA airport code
		destination.readFields(dataInput);// IATA airport code
		taxiIn.readFields(dataInput);// taxi in time, in minutes
		taxiOut.readFields(dataInput);// taxi out time in minutes
		cancelled.readFields(dataInput);//
		cancellationCode.readFields(dataInput);
		// reason for cancellation (A = carrier, B = weather, C = NAS, D =
		// security)
		diverted.readFields(dataInput);// 1 = yes, 0 = no
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		//flightDate.write(dataOutput);
		uniqueCarrier.write(dataOutput);
		flightNum.write(dataOutput);// flight number
		arrivalDelay.write(dataOutput);// arrival delay, in minutes
		departureDelay.write(dataOutput);// departure delay, in minutes
		origin.write(dataOutput);// IATA airport code
		destination.write(dataOutput);// IATA airport code
		taxiIn.write(dataOutput);// taxi in time, in minutes
		taxiOut.write(dataOutput);// taxi out time in minutes
		cancelled.write(dataOutput);//
		cancellationCode.write(dataOutput);
		// reason for cancellation (A = carrier, B = weather, C = NAS, D =
		// security)
		diverted.write(dataOutput);// 1 = yes, 0 = no
	}

	@Override
	public int hashCode() {
		int result = (this.getFlightNum() != null ? this.getFlightNum().hashCode() : 0) + (this.getFlightDate() != null ? this.getFlightDate().hashCode() : 0)
				+ (this.getDestination() != null ? this.getDestination().hashCode() : 0);
		return result;
	}

	@Override
	public boolean equals(Object that) {
		if (null == that || getClass() != that.getClass())
			return false;

		if (this == that)
			return true;

		HadoopFlight flight = (HadoopFlight) that;

		if (flight != null) {
			if (!this.flightDate.equals(flight.getFlightDate()))
				return false;
			if (!this.flightNum.equals(flight.getFlightNum()))
				return false;
			if (!this.destination.equals(flight.getDestination()))
				return false;
		}

		return true;
	}

	public Boolean isOnSchedule() {
		return (arrivalDelay.get() < 11) && (departureDelay.get() < 11);
	}

	public Boolean isCancelled() {
		return (cancelled.get() == true);
	}

}
