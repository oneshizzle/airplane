package edu.njit.an395.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.njit.an395.combine.FlightCombine;
import edu.njit.an395.hadoop.FlightScheduleStats;
import edu.njit.an395.map.FlightMap;
import edu.njit.an395.partition.HadoopFlightPartitioner;
import edu.njit.an395.reduce.FlightOnScheduleReduce;

public class FlightOnScheduleDriver {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(FlightOnScheduleDriver.class);
		job.setJobName("FlightOnScheduleProbabilty");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(FlightMap.class);
		job.setReducerClass(FlightOnScheduleReduce.class);
		job.setCombinerClass(FlightCombine.class);
		job.setPartitionerClass(HadoopFlightPartitioner.class);
		// job.setSortComparatorClass(WordPairComparator.class);
		job.setNumReduceTasks(2);

		job.setOutputKeyClass(FlightScheduleStats.class);
		job.setOutputValueClass(LongWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
