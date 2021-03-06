package edu.njit.an395.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.njit.an395.map.AirportTaxiOutMap;
import edu.njit.an395.reduce.AvgAirportTaxiReduce;

public class AirportAvgTaxiOutDriver {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(AirportAvgTaxiOutDriver.class);
		job.setJobName("AirportAvgTaxiOut");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(AirportTaxiOutMap.class);
		job.setReducerClass(AvgAirportTaxiReduce.class);
		job.setNumReduceTasks(2);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
