package com.ranksays.rocksdb.client;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.junit.Test;

public class RocksDBTest {
	@Test
	public void testBasic() throws IOException {
		String db = "test";
		String username = "username";
		String password = "password";

		RocksDB rdb = new RocksDB(username, password);

		byte[] key = "k".getBytes();
		byte[] value = "v".getBytes();

		// get
		assertNull(rdb.get(db, key));

		// put
		rdb.put(db, key, value);
		assertArrayEquals(value, rdb.get(db, key));

		// remove
		rdb.remove(db, key);
		assertNull(rdb.get(db, key));

		rdb.dropDatabase(db);
	}

	@Test
	public void testBatch() throws IOException {
		String db = "test";
		String username = "username";
		String password = "password";

		RocksDB rdb = new RocksDB(username, password);

		byte[][] keys1 = { "k1".getBytes(), "k2".getBytes(), "k3".getBytes() };
		byte[][] keys2 = { keys1[0], keys1[2] };
		byte[][] values1 = { "k1".getBytes(), "k2".getBytes(), "k3".getBytes() };
		byte[][] values2 = { null, values1[1], null };

		// batch put
		rdb.putBatch(db, keys1, values1);

		// batch get
		byte[][] values = rdb.getBatch(db, keys1);
		for (int i = 0; i < values1.length; i++) {
			assertArrayEquals(values1[i], values[i]);
		}

		// batch remove
		rdb.removeBatch(db, keys2);

		// batch get
		values = rdb.getBatch(db, keys1);
		for (int i = 0; i < values2.length; i++) {
			assertArrayEquals(values2[i], values[i]);
		}

		rdb.dropDatabase(db);
	}
}
