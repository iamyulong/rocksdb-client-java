# rocksdb-client-java

This is a Java client library for [RocksDB Server](https://github.com/iamyulong/rocksdb-server).

## How to use

1. Add the following repository:

    ```
    <repositories>
        <repository>
            <id>jitpack.io</id>
            <url>https://jitpack.io</url>
        </repository>
    </repositories>
    ```

2. Import the library:

    ```
    <dependency>
        <groupId>com.github.iamyulong</groupId>
        <artifactId>rocksdb-client-java</artifactId>
        <version>2.0</version>
    </dependency>
    ```

## Example code

```java
import com.ranksays.rocksdb.client.RocksDB;

public class Main {

    public static void main(String[] args) throws Exception {

        RocksDB rdb = new RocksDB("localhost", 8516, "username", "password");

        String db = "test";
        String key = "key";
        String value = "value";

        // create database (optional)
        rdb.createDatabase(db);

        // put
        rdb.put(db, key, value);

        // get
        System.out.println(rdb.get(db, key));

        // delete
        rdb.delete(db, key);

        // dump stats
        System.out.println(rdb.getStats(db));

        // drop database
        rdb.dropDatabase(db);
    }
}
```