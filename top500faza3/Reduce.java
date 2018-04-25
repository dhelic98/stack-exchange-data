package top500faza3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text, Text, Text> {
	public static int counter = 0;

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		int numofacc = 0;
		int numofans = 0;
		for (Text value : values) {
			String line = value.toString();
			String[] parts = line.split("\t");
			int num = Integer.parseInt(parts[0].trim());
			sum += num;
			num = Integer.parseInt(parts[1].trim());
			numofacc += num;
			num = Integer.parseInt(parts[2].trim());
			numofans += num;

		}

		Text txt = new Text(sum + "\t" + numofacc + "\t" + numofans);

		context.write(key, txt);

	}

}
