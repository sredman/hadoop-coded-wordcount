import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.ToolRunner;

public class WordCount {

	private static Map<GroupedWord, Integer> locallySeenValues;

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
		private static final Log LOG = LogFactory.getLog(WordCountCombiner.class);

		private BroadcastValue broadcastValue = new BroadcastValue();
		
		private Partitioner<GroupedWord, BroadcastValue> partitioner = new HashPartitioner<>();

		// Keep a node-local mapping of words to values
		// When we see a chance to send an encoded broadcast packet (based on querying this cache), send it
		private Map<GroupedWord, List<BroadcastValue>> cachedValues;

		@Override
		public void setup(Context context) {
			cachedValues = new TreeMap<>();
			locallySeenValues = new TreeMap<>();
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
			if (locallySeenValues.containsKey(key)) {
				// This should never happen, but I could be wrong
				int oldCount = locallySeenValues.get(key);
				int newCount = oldCount + sum;
				LOG.info("Unexpectedly appending to existing locally-seen value from Collector. Previously had " + oldCount + ", now have " + newCount);
				locallySeenValues.put(key, newCount);
			} else {
				locallySeenValues.put(key, sum);
			}
		}

		private void cache(GroupedWord key, BroadcastValue val) {
			BroadcastValue cacheableValue = new BroadcastValue(val);
			if (!cachedValues.containsKey(key)) {
				GroupedWord cacheableKey = new GroupedWord(key);
				cachedValues.put(cacheableKey, new LinkedList<>());
			}
			cachedValues.get(key).add(cacheableValue);
		}

		private void cacheOrSend(GroupedWord key, BroadcastValue val, Context context) throws IOException, InterruptedException {
			// In order to be able to encode, we need:
			// - A key which is shared with this (A) and one other node (B), whose value targets a third node (C)
			// - A key which is shared with node A and node C whose value is targeting node B
			// Luckily, we can do away with one check: We don't care which two nodes are targeted for the Reduce step,
			// we just care that two different nodes are targeted, because Hadoop tosses everything into one big bucket anyway

			int newValueTarget = partitioner.getPartition(key, val, 3);
			List<String> newValueLocations = Arrays.asList(key.getLocations());

			if (newValueLocations.size() != 2) {
				LOG.info("Only able to handle keys which reside on two nodes, this was on " + newValueLocations.size());
				cache(key, val);
				return;
			}
			// Find a key which addresses one node that this key does not and one that it does
			// ASSUMPTION: There are only and exactly three nodes in the cluster, ever key resides on exactly two nodes
			GroupedWord combinableKey = null;
			for (GroupedWord cachedKey : cachedValues.keySet()) {
				if (cachedValues.get(cachedKey).size() == 0) {
					// Even if this is a target to which we could encode, there is no data
					// to encode with
					continue;
				}

				List<String> cachedValueLocations = Arrays.asList(cachedKey.getLocations());

				int cachedValueTarget = partitioner.getPartition(cachedKey, null, 3);

				if (newValueTarget == cachedValueTarget) {
					// Targeting same node. No way to encode.
					continue;
				}

				if (newValueLocations.size() != cachedValueLocations.size()) {
					LOG.info("Not able to handle keys which reside on differing numbers of nodes");
					continue;
				}
				// If all targets are overlapping, then the targets list is the same and there is no chance to encode
				// If all but one target is overlapping, we can encode
				List<String> overlappingLocations = new LinkedList<String>();
				for (String newLocation : newValueLocations) {
					for (String cachedLocation : cachedValueLocations) {
						if (newLocation.equals(cachedLocation)) {
							overlappingLocations.add(newLocation);
						}
					}
				}

				LOG.debug("Found potentially encode-able key which overlaps at " + overlappingLocations.size() + " locations");
				// Hardcoded for 3 nodes and 2 copies in following block
				if (overlappingLocations.size() == 1) {
					// Can possibly encode! Check targets
					combinableKey = cachedKey;
					break;
				} else {
					// Same locations; cannot encode
					continue;
				}
			}

			if (combinableKey == null) {
				cache(key, val);
				return;
			}

			BroadcastValue encodingValue = cachedValues.get(combinableKey).remove(0);
			assert encodingValue.words.size() == 0 : "Too many values already in target";
			assert val.words.size() == 0 : "Too many values in source";

			BroadcastValue encodedValue = new BroadcastValue();
			encodedValue.set(
					Arrays.asList(val.words.get(0), encodingValue.words.get(0)), // Target both words
					val.value ^ encodingValue.value // Use simple XOR encoding
					);

			// Cheat: Hadoop does not provide broadcast (as far as I'm aware) so send one packet
			// of each key which will be delivered unicast to each node, but we'll count it as one
			context.write(key, encodedValue);
			context.write(combinableKey, encodedValue);
			// Record the fact that we sent *only one* packet
			context.getCounter(WordCountDriver.COMMUNICATION_LOAD_COUNTER.PACKETS_SENT).increment(1);
			context.getCounter(WordCountDriver.COMMUNICATION_LOAD_COUNTER.ENCODED_PACKETS_SENT).increment(1);
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			assert cachedValues.keySet().size() == 1 : "Cached values could have been combined";
			for (GroupedWord key : cachedValues.keySet()) {
				for (BroadcastValue val : cachedValues.get(key)) {
					assert val.words.size() == 1 : "Cached value was ready to be delivered";
					assert key.getWord() == val.words.get(0) : "Key does not match value";
					context.write(key, val);
					// Record the fact that we sent a packet
					context.getCounter(WordCountDriver.COMMUNICATION_LOAD_COUNTER.PACKETS_SENT).increment(1);
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
				sum += decode(key, val);
			}
			result.set(sum);
			word.set(key.getWord());
			context.write(word, result);
		}

		private int decode(GroupedWord key, BroadcastValue val) {
			if (val.words.size() == 1) {
				// Not encoded
				return val.value;
			}

			if (val.words.size() > 2) {
				throw new NotImplementedException("Unable to handle encoded value with more than two parts");
			}
			String soughtWord = key.getWord();
			// We know the word which is not the one we are seeking
			String knownWord = !val.words.get(0).equals(soughtWord) ? val.words.get(0) : val.words.get(1);
			GroupedWord knownKey = new GroupedWord(key);
			knownKey.set(knownKey.getLocations(), knownWord);

			int knownVal = locallySeenValues.get(key);
			return val.value ^ knownVal;
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