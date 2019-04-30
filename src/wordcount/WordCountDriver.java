package wordcount;
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
		ENCODED_PACKETS_SENT,
	}

	public Class<? extends Mapper<Object, Text, GroupedWord, BroadcastValue>> mapper;
	
	// Combiner runs a reduce-like operation to collect the outputs of the map operation
	// In this phase, examine how the file is split and "broadcast" it to all nodes
	public Class<? extends Reducer<GroupedWord, BroadcastValue, GroupedWord, BroadcastValue>> combiner;
	
	public Class<? extends Reducer<GroupedWord,BroadcastValue,Text,IntWritable>> reducer;

	// The partitioner assigns keys to reducer nodes. In this phase, sort non-broadcast keys
	// normally, but make sure to send "broadcast" packets to their specified location
	public Class<? extends Partitioner<Text, IntWritable>> partitioner;

	WordCountDriver(
			Class<? extends Mapper<Object, Text, GroupedWord, BroadcastValue>> mapper,
			Class<? extends Reducer<GroupedWord, BroadcastValue, GroupedWord, BroadcastValue>> combiner,
			Class<? extends Reducer<GroupedWord, BroadcastValue, Text, IntWritable>> reducer) {
		this.mapper = mapper;
		this.combiner = combiner;
		this.reducer = reducer;
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");

		job.setJarByClass(WordCount.class);
		job.setMapperClass(mapper);
		job.setCombinerClass(combiner);
		job.setReducerClass(reducer);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapOutputKeyClass(GroupedWord.class);
		job.setMapOutputValueClass(BroadcastValue.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
}
