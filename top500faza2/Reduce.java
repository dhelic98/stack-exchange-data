package top500faza2;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<IntWritable, Text, Text, IntWritable> {
	private PriorityQueue<Integer> queue = new PriorityQueue<>();
	private PriorityQueue<Text> queue2 = new PriorityQueue<>();

	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text value : values) {

			String s = value.toString();
			Integer i = queue.peek();
			Text t = queue2.peek();
			if ((queue.size() <= 500) || (key.get() > i)) {
				queue.add(key.get());
				queue2.add(new Text(s));

				if (queue.size() > 500) {
					queue.remove(i);
					queue2.remove(t);
				}

			}

		}

		while (!queue.isEmpty()) {
			context.write(queue2.poll(), new IntWritable(queue.poll()));
		}

	}

}
