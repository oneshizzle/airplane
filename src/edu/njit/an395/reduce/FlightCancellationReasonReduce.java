package edu.njit.an395.reduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlightCancellationReasonReduce extends Reducer<Text, LongWritable, Text, Text> {

	private HashMap<Text, LongWritable> cancellationCodeMap;

	@Override
	protected void reduce(Text cancellationCode, Iterable<LongWritable> flights, Context context) throws IOException, InterruptedException {
		LongWritable sum = new LongWritable(0);

		if (null == cancellationCodeMap) {
			cancellationCodeMap = new HashMap<Text, LongWritable>();
		}

		Iterator<LongWritable> iter = flights.iterator();
		while (iter.hasNext()) {
			LongWritable value = iter.next();
			sum.set(sum.get() + value.get());
		}

		cancellationCodeMap.put(cancellationCode, sum);

	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		Set<Text> codes = cancellationCodeMap.keySet();
		LongWritable maxValue = new LongWritable(0);
		Text cancellationCode = new Text();

		for (Text code : codes) {
			if (cancellationCodeMap.get(code).get() > maxValue.get()) {
				maxValue.set(cancellationCodeMap.get(code).get());
				cancellationCode.set(code.toString());
			}
		}
		context.write(cancellationCode, new Text(cancellationCode + " " + maxValue.get()));

	}
}
