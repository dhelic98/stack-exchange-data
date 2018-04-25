package top500;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	private IntWritable num = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		if (line.contains("Tags=")) {
			String[] parts = line.split("Tags=\"");
			int start = 0;
			int end = parts[1].indexOf("\"");
			String tags = parts[1].substring(start, end);
			tags = tags.replaceAll("&lt;", "");
			tags = tags.replaceAll("&gt;", " ");
			parts = tags.split(" ");
			Text word = new Text();
			for (String part : parts) {
				word.set(part.trim());
				context.write(word, num);
			}

		} else {
			return;
		}
	}

}
