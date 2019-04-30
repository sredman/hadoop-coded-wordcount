package wordcount;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class BroadcastValue implements Writable {
	
	/**
	 * The keys which are "encoded" in this packet
	 */
	List<String> words;
	
	/**
	 * Encoded value
	 */
	int value;
	
	public BroadcastValue() {}
	
	/**
	 * Copy Constructor
	 */
	public BroadcastValue(BroadcastValue other) {
		this.words = new LinkedList<>(other.words);
		this.value = other.value;
	}
	
	public void set(List<String> words, int value) {
		this.words = words;
		this.value = value;
	}
	
	public int get() {
		return value;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		int numKeys = words.size();
		
		out.writeInt(numKeys);
		for (String key : words) {
			out.writeUTF(key);
		}
		out.writeInt(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int numKeys = in.readInt();
		
		words = new LinkedList<String>();
		for (int index = 0; index < numKeys; index ++) {
			words.add(in.readUTF());
		}
		value = in.readInt();
	}
	
	@Override
	public String toString() {
		return "Broadcast: " + Arrays.toString(words.toArray());
	}

}
