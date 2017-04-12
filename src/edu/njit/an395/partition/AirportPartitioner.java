package edu.njit.an395.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import edu.njit.an395.hadoop.HadoopFlight;

public class AirportPartitioner extends Partitioner<HadoopFlight, LongWritable> {

	@Override
	public int getPartition(HadoopFlight aHadoopFlight, LongWritable longWritable, int numPartitions) {
		return Math.abs(aHadoopFlight.getDestination().hashCode()) % numPartitions;
	}
}