package top500faza2;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, IntWritable, Text> {

	private PriorityQueue<Integer> queue = new PriorityQueue<>();
	private PriorityQueue<String> queue2 = new PriorityQueue<>();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] parts = line.split("\t");
		Integer num = Integer.parseInt(parts[1].trim());

		if (parts.length == 2) {
			if (queue.size() <= 500 || num > queue.peek()) {
				queue.add(num);
				queue2.add(parts[0].trim());

				if (queue.size() > 500) {
					queue.poll();
					queue2.poll();
				}
			}

		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		while (!queue.isEmpty()) {
			context.write(new IntWritable(queue.poll()), new Text(queue2.poll()));
		}

	}

}
