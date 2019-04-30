package test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.Test;

import wordcount.GroupedWord;

class GroupedWordTests {
	String[] locations = { "loc1", "loc2" };

	@Test
	void encodeDecodeTest() throws IOException {
		GroupedWord word = new GroupedWord();
		word.set(locations, "word");

		InputStream in = new PipedInputStream();
		OutputStream out = new PipedOutputStream((PipedInputStream)in);

		DataInput dataIn = new DataInputStream(in);
		DataOutput dataOut = new DataOutputStream(out);

		word.write(dataOut);

		GroupedWord decodedWord = new GroupedWord();
		decodedWord.readFields(dataIn);

		assertNotSame(word, decodedWord);
		assertNotSame(word.getLocations(), decodedWord.getLocations());

		assertEquals(word, decodedWord);
		assertArrayEquals(word.getLocations(), decodedWord.getLocations());
	}

	@Test
	void copyConstructorTest() {
		GroupedWord word = new GroupedWord();
		word.set(locations, "word");

		GroupedWord copy = new GroupedWord(word);
		assertNotSame(word, copy);
		assertNotSame(word.getLocations(), copy.getLocations());

		assertEquals(word, copy);
		assertArrayEquals(word.getLocations(), copy.getLocations());
	}

	@Test
	void hashCodeTest() {
		Random rand = new Random();
		Set<Integer> seenCodes = new HashSet<Integer>();

		for (int index = 0; index < 10000; index++) {
			String loc1 = Integer.toString(rand.nextInt());
			String loc2 = Integer.toString(rand.nextInt());
			String word = Integer.toString(rand.nextInt());

			GroupedWord test = new GroupedWord();
			test.set(new String[] {loc1, loc2}, word);

			int hashCode = test.hashCode();
			if (seenCodes.contains(hashCode)) {
				fail("Duplicate hash after " + index + " attempts? That seems improbable!");
			}
			seenCodes.add(hashCode);
		}
	}
}
