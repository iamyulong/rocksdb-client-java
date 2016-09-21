package com.ranksays.rocksdb.client;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.junit.Test;

public class RocksDBTest {
	@Test
	public void testAll() throws IOException {
		String db = "test";
		String username = "username";
		String password = "password";

		RocksDB rdb = new RocksDB(username, password);

		byte[] key = "k".getBytes();
		byte[] value = "v".getBytes();

		assertNull(rdb.get(db, key));

		rdb.put(db, key, value);
		assertArrayEquals(value, rdb.get(db, key));

		rdb.remove(db, key);
		assertNull(rdb.get(db, key));

		rdb.dropDatabase(db);
	}
}
