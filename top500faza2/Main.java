package top500faza2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int exitcode = ToolRunner.run(new Main(), args);
		System.exit(exitcode);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf);
		job.setJobName("top500keywords");
		job.setJarByClass(Main.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setSortComparatorClass(DescendingComparator.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);
		Path inputFilePath2 = new Path(args[2]);

		FileInputFormat.setInputPaths(job, inputFilePath);
		FileInputFormat.setInputPaths(job, inputFilePath2);
		FileOutputFormat.setOutputPath(job, outputFilePath);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

}
