import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;

public class WordCount {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}

		@Override
		public void cleanup(Context context) {
			// Called once at the end of the task. Use to output any keys which we were waiting on
		}
	}

	public static class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);

			// TODO: Broadcast here
			locallyMappedValues.put(key.toString(), sum);

			// Record the fact that we "sent" a packet
			context.getCounter(WordCountDriver.COMMUNICATION_LOAD_COUNTER.PACKETS_SENT).increment(1);
		}

		@Override
		public void cleanup(Context context) {
			// TODO: Output any potentially-encode-able messages which were not actually encoded
			return;
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static void main (String[] args) throws Exception {
		WordCountDriver driver = new WordCountDriver(
				WordCount.TokenizerMapper.class,
				WordCount.IntSumReducer.class,
				WordCount.WordCountCombiner.class);
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}