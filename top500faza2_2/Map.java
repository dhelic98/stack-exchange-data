package top500faza2_2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {


	private static ArrayList<String> tags = new ArrayList<>();
	private static int counter = 0;
	private static StringBuffer sb = new StringBuffer();

	public static void load(ArrayList<String> tags1) throws IOException {
		tags.addAll(tags1);
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();

		sb.append(line + " ");
		if (line.contains("/>")) {
			String text = sb.toString();
			sb = new StringBuffer();
			String[] parts = text.split("Body=\"");
			if (parts.length == 2) {

				text = parts[1];

				String[] words = text.split(" ");

				for (String tag : tags) {
					for (String word : words) {
						if (word.contains(tag)) {
							counter++;
						}
					}

					if (counter > 0) {
						context.write(new Text(tag), new IntWritable(counter));
						counter = 0;
					}

				}
			} else {
				return;
			}

		}

	}
}
