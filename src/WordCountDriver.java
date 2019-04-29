import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class WordCountDriver extends Configured implements Tool {
	
	public static enum COMMUNICATION_LOAD_COUNTER {
		PACKETS_SENT,
	}

	public Class <? extends Mapper<Object, Text, Text, IntWritable>> mapper;
	public Class <? extends Reducer<Text, IntWritable, Text, IntWritable>> reducer;

	// Combiner runs a reduce-like operation to collect the outputs of the map operation
	// In this phase, examine how the file is split and "broadcast" it to all nodes
	public Class <? extends Reducer<Text, IntWritable, Text, IntWritable>> combiner;

	// The partitioner assigns keys to reducer nodes. In this phase, sort non-broadcast keys
	// normally, but make sure to send "broadcast" packets to their specified location
	public Class <? extends Partitioner<Text, IntWritable>> partitioner;

	WordCountDriver(
			Class<? extends Mapper<Object, Text, Text, IntWritable>> mapper,
			Class<? extends Reducer<Text, IntWritable, Text, IntWritable>> reducer,
			Class<? extends Reducer<Text, IntWritable, Text, IntWritable>> combiner) {
		this.mapper = mapper;
		this.reducer = reducer;
		this.combiner = combiner;
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");

		job.setJarByClass(WordCount.class);
		job.setMapperClass(mapper);
		job.setCombinerClass(reducer);
		job.setReducerClass(combiner);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
}
