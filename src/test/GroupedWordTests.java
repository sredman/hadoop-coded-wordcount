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
}
