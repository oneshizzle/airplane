package edu.njit.an395;

public class RawFlight {

	private String year; // 1987-2008
	private String month; // 1-12
	private String dayOfMonth; // 1-31
	private String dayOfWeek;
	private String actualDepTime;// actual departure time (local, hhmm)
	private String listedDepTime;// scheduled departure time (local, hhmm)
	private String actualArrivalTime;// actual arrival time (local, hhmm)
	private String listedArrTime;// scheduled arrival time (local, hhmm)
	private String uniqueCarrier;// unique carrier code
	private String flightNum;// flight number
	private String tailNum;
	private short actualElapsedTime;// in minutes
	private short listedElapsedTime;// in minutes
	private short airTime;// in minutes
	private short arrivalDelay;// arrival delay, in minutes
	private short departureDelay;// departure delay, in minutes
	private String origin;// IATA airport code
	private String destination;// IATA airport code
	private short distance;// in miles
	private String taxiIn;// taxi in time, in minutes
	private String taxiOut;// taxi out time in minutes
	private short cancelled;//
	private String cancellationCode;
	// reason for cancellation (A = carrier, B = weather, C = NAS, D = security)
	private short diverted;// 1 = yes, 0 = no
	// NA below
	private String carrierDelay;// in minutes
	private String weatherDelay;// in minutes
	private String nasDelay; // in minutes
	private String securityDelay;// in minutes
	private String lateAircraftDelay; // in minutes

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public String getDayOfMonth() {
		return dayOfMonth;
	}

	public void setDayOfMonth(String dayOfMonth) {
		this.dayOfMonth = dayOfMonth;
	}

	public String getDayOfWeek() {
		return dayOfWeek;
	}

	public void setDayOfWeek(String dayOfWeek) {
		this.dayOfWeek = dayOfWeek;
	}

	public String getActualDepTime() {
		return actualDepTime;
	}

	public void setActualDepTime(String actualDepTime) {
		this.actualDepTime = actualDepTime;
	}

	public String getListedDepTime() {
		return listedDepTime;
	}

	public void setListedDepTime(String listedDepTime) {
		this.listedDepTime = listedDepTime;
	}

	public String getActualArrivalTime() {
		return actualArrivalTime;
	}

	public void setActualArrivalTime(String actualArrivalTime) {
		this.actualArrivalTime = actualArrivalTime;
	}

	public String getListedArrTime() {
		return listedArrTime;
	}

	public void setListedArrTime(String listedArrTime) {
		this.listedArrTime = listedArrTime;
	}

	public String getUniqueCarrier() {
		return uniqueCarrier;
	}

	public void setUniqueCarrier(String uniqueCarrier) {
		this.uniqueCarrier = uniqueCarrier;
	}

	public String getFlightNum() {
		return flightNum;
	}

	public void setFlightNum(String flightNum) {
		this.flightNum = flightNum;
	}

	public String getTailNum() {
		return tailNum;
	}

	public void setTailNum(String tailNum) {
		this.tailNum = tailNum;
	}

	public short getActualElapsedTime() {
		return actualElapsedTime;
	}

	public void setActualElapsedTime(short actualElapsedTime) {
		this.actualElapsedTime = actualElapsedTime;
	}

	public short getListedElapsedTime() {
		return listedElapsedTime;
	}

	public void setListedElapsedTime(short listedElapsedTime) {
		this.listedElapsedTime = listedElapsedTime;
	}

	public short getAirTime() {
		return airTime;
	}

	public void setAirTime(short airTime) {
		this.airTime = airTime;
	}

	public short getArrivalDelay() {
		return arrivalDelay;
	}

	public void setArrivalDelay(short arrivalDelay) {
		this.arrivalDelay = arrivalDelay;
	}

	public short getDepartureDelay() {
		return departureDelay;
	}

	public void setDepartureDelay(short departureDelay) {
		this.departureDelay = departureDelay;
	}

	public String getOrigin() {
		return origin;
	}

	public void setOrigin(String origin) {
		this.origin = origin;
	}

	public String getDestination() {
		return destination;
	}

	public void setDestination(String destination) {
		this.destination = destination;
	}

	public short getDistance() {
		return distance;
	}

	public void setDistance(short distance) {
		this.distance = distance;
	}

	public String getTaxiIn() {
		return taxiIn;
	}

	public void setTaxiIn(String taxiIn) {
		this.taxiIn = taxiIn;
	}

	public String getTaxiOut() {
		return taxiOut;
	}

	public void setTaxiOut(String taxiOut) {
		this.taxiOut = taxiOut;
	}

	public short getCancelled() {
		return cancelled;
	}

	public void setCancelled(short cancelled) {
		this.cancelled = cancelled;
	}

	public String getCancellationCode() {
		return cancellationCode;
	}

	public void setCancellationCode(String cancellationCode) {
		this.cancellationCode = cancellationCode;
	}

	public short getDiverted() {
		return diverted;
	}

	public void setDiverted(short diverted) {
		this.diverted = diverted;
	}

	public String getCarrierDelay() {
		return carrierDelay;
	}

	public void setCarrierDelay(String carrierDelay) {
		this.carrierDelay = carrierDelay;
	}

	public String getWeatherDelay() {
		return weatherDelay;
	}

	public void setWeatherDelay(String weatherDelay) {
		this.weatherDelay = weatherDelay;
	}

	public String getNasDelay() {
		return nasDelay;
	}

	public void setNasDelay(String nasDelay) {
		this.nasDelay = nasDelay;
	}

	public String getSecurityDelay() {
		return securityDelay;
	}

	public void setSecurityDelay(String securityDelay) {
		this.securityDelay = securityDelay;
	}

	public String getLateAircraftDelay() {
		return lateAircraftDelay;
	}

	public void setLateAircraftDelay(String lateAircraftDelay) {
		this.lateAircraftDelay = lateAircraftDelay;
	}

	public String toString() {
		return this.getUniqueCarrier() + " " + this.getFlightNum();
	}
}
