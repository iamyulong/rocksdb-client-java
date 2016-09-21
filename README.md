# rocksdb-client-java
Java client for RocksDB Server

## How to use

1. Download the latest release from <https://github.com/ywu123/rocksdb-client-java/releases>.
2. Import the jar file to your project. You can either use the 'with-dependencies' version or the bare version.

## Usage example

```java
import java.io.IOException;

import com.ranksays.rocksdb.client.RocksDB;

public class Main {

	public static void main(String[] args) throws IOException {

		RocksDB rdb = new RocksDB("localhost", 8516, true, "username", "password");

		String db = "test";
		byte[] key = "k".getBytes();
		byte[] value = "v".getBytes();

		// put
		rdb.put(db, key, value);

		// get
		System.out.println(new String(rdb.get(db, key)));

		// remove
		rdb.remove(db, key);

		// drop database
		rdb.dropDatabase(db);
	}

}
```