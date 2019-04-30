import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;

public class WordCount {

	public static class TokenizerMapper extends Mapper<Object, Text, GroupedWord, BroadcastValue> {
		private GroupedWord groupedWord = new GroupedWord();
		private BroadcastValue unicastValue = new BroadcastValue();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String word = itr.nextToken();
				groupedWord.set(context.getInputSplit().getLocations(), word);
				unicastValue.set(Arrays.asList(word), 1);
				context.write(groupedWord, unicastValue);
			}
		}

		@Override
		public void cleanup(Context context) {
			// Called once at the end of the task. Use to output any keys which we were waiting on
			return;
		}
	}

	/**
	 * Handles the Collect stage after the Map phase where locally-counted values are collected
	 * per-key
	 */
	public static class WordCountCombiner extends Reducer<GroupedWord, BroadcastValue, GroupedWord, BroadcastValue> {
		private BroadcastValue broadcastValue = new BroadcastValue();
		
		// Keep a node-local mapping of words to values
		// When we see a chance to send an encoded broadcast packet (based on querying this cache), send it
		private Map<GroupedWord, List<BroadcastValue>> cachedValues;

		@Override
		public void setup(Context context) {
			cachedValues = new TreeMap<>();
			return;
		}

		@Override
		public void reduce(GroupedWord key, Iterable<BroadcastValue> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			List<String> words = new LinkedList<>();
			for (BroadcastValue val : values) {
				sum += val.get();
				words = val.words;
			}
			broadcastValue.set(words, sum);
			cacheOrSend(key, broadcastValue, context);
		}

		private void send(GroupedWord key, BroadcastValue val, Context context) throws IOException, InterruptedException {
			context.write(key, val);
			// Record the fact that we "sent" a packet
			context.getCounter(WordCountDriver.COMMUNICATION_LOAD_COUNTER.PACKETS_SENT).increment(1);
		}

		private void cache(GroupedWord key, BroadcastValue val) {
			BroadcastValue cacheableValue = new BroadcastValue(val);
			if (!cachedValues.containsKey(key)) {
				GroupedWord cacheableKey = new GroupedWord(key);
				cachedValues.put(cacheableKey, new LinkedList<>());
			}
			cachedValues.get(key).add(cacheableValue);
		}

		private void cacheOrSend(GroupedWord key, BroadcastValue val, Context context) {
			cache(key, val);
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			assert cachedValues.keySet().size() == 1 : "Cached values could have been combined";
			for (GroupedWord key : cachedValues.keySet()) {
				for (BroadcastValue val : cachedValues.get(key)) {
					assert val.words.size() == 1 : "Cached value was ready to be delivered";
					assert key.getWord() == val.words.get(0) : "Key does not match value";
					send(key, val, context);
				}
			}
		}
	}

	public static class IntSumReducer extends Reducer<GroupedWord, BroadcastValue, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		private Text word = new Text();

		@Override
		public void reduce(GroupedWord key, Iterable<BroadcastValue> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (BroadcastValue val : values) {
				//TODO: Unpack and decode values
				sum += val.get();
			}
			result.set(sum);
			word.set(key.getWord());
			context.write(word, result);
		}
	}
	
	public static void main (String[] args) throws Exception {
		WordCountDriver driver = new WordCountDriver(
				WordCount.TokenizerMapper.class,
				WordCount.WordCountCombiner.class,
				WordCount.IntSumReducer.class);
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}