import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.WritableComparable;

/**
 * Simple key class which pairs a word with the input file partition that it came from
 */
public class GroupedWord implements WritableComparable<GroupedWord> {

	private String[] splitLocations;
	private String word;
	
	public GroupedWord() {}
	
	public GroupedWord(GroupedWord other) {
		this.splitLocations = Arrays.copyOf(other.splitLocations, other.splitLocations.length);
		this.word = other.word;
	}
	
	public String getWord() {
		return word;
	}
	
	public void set(String[] splitLocations, String word) {
		this.splitLocations = splitLocations;
		this.word = word;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		int numSplits = splitLocations.length;
		out.writeInt(numSplits);
		for (String location : splitLocations) {
			out.writeUTF(location);
		}
		out.writeUTF(word);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int numSplits = in.readInt();
		splitLocations = new String[numSplits];
		for (int index = 0; index < numSplits; index++) {
			splitLocations[index] = in.readUTF();
		}
		word = in.readUTF();
	}

	@Override
	public int compareTo(GroupedWord other) {
		for (int index = 0; splitLocations != null && index < splitLocations.length; index++) {
			int comparison = this.splitLocations[index].compareTo(other.splitLocations[index]);
			if (comparison != 0) {
				return comparison;
			}
		}
		return this.word.compareTo(other.word);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof GroupedWord)) {
			return false;
		}
		GroupedWord other = (GroupedWord) obj;
		
		return this.compareTo(other) == 0;
	}
	
	@Override
	public int hashCode() {
		int locationHashes = 0;
		for (String location : this.splitLocations) {
			locationHashes += location.hashCode();
		}
		locationHashes += 1; // Start at 1 to make sure multiplication is at least identity
		return word.hashCode() * locationHashes;
	}

}
